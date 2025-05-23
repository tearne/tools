use std::{fs::DirEntry, io::Write, path::Path};

use aws_sdk_s3::{operation::{list_object_versions::ListObjectVersionsOutput, list_objects_v2::ListObjectsV2Output}, primitives::{ByteStream, SdkBody}, types::{BucketVersioningStatus, Delete, Object, ObjectIdentifier, ObjectVersion}, Client};
use human_format::Formatter;

use tokio::runtime::Handle;
use color_eyre::{eyre::{eyre, Context, OptionExt}, Result};

use super::types::S3Location;

pub struct S3Wrapper {
    pub client: Client
}

impl S3Wrapper {
    pub async fn get_object_versions(&self, bucket: &str, prefix: &str, verbose: bool) -> Result<Vec<ObjectVersion>> {
        let pages = self.get_versions(bucket, prefix, verbose).await.unwrap();
        let object_versions: Vec<ObjectVersion> = pages.into_iter()
            .flat_map(|page|
                page.versions.unwrap_or_default())
            .collect();

        Ok(object_versions)
    }

    pub async fn list_objects_v2(&self, bucket: &str, prefix: &str) -> Result<Vec<Object>> {
        let mut acc: Vec<Object> = Vec::new();

        async fn next_page(
            client: &Client,
            bucket: &str,
            prefix: &str,
            c_tok: Option<String>,
        ) -> Result<ListObjectsV2Output> {
            client
                .list_objects_v2()
                .bucket(bucket)
                .prefix(prefix)
                .set_continuation_token(c_tok)
                .send()
                .await
                .map_err(|e| e.into())
        }

        let mut c_token = None;
        loop {
            let list_output = next_page(&self.client, bucket, prefix, c_token).await?;

            c_token = list_output.next_continuation_token().map(str::to_string);

            if let Some(mut items) = list_output.contents {
                acc.append(&mut items);
            }

            if c_token.is_none() {
                break;
            }
        }

        Ok(acc)
    }

    pub async fn is_versioning_enabled(&self, bucket: &str) -> Result<bool> {
        self
            .client
            .get_bucket_versioning()
            .bucket(bucket) 
            .send()
            .await?
            .status
            .map(|s| s == BucketVersioningStatus::Enabled)
            .ok_or_eyre("Error during version checking")
    }

    // TODO combine with pub above?
    async fn get_versions(&self, bucket: &str, prefix: &str, verbose: bool) -> Result<Vec<ListObjectVersionsOutput>> {
        async fn next_page(
            client: &Client,
            bucket: &str,
            prefix: &str,
            next_key: Option<String>,
            next_version: Option<String>,
        ) -> Result<ListObjectVersionsOutput> {
            client
                .list_object_versions()
                .bucket(bucket)
                .prefix(prefix)
                .set_key_marker(next_key)
                .set_version_id_marker(next_version)
                .send()
                .await
                .map_err(|e| e.into())
        }

        let mut next_key = None;
        let mut next_version = None;

        let mut acc: Vec<ListObjectVersionsOutput> = Vec::new();
        let mut prev_records_counter: usize = 0;
        let mut formatter = Formatter::new();
        formatter.with_decimals(1);

        if verbose {print!("Requesting version pages ...")};
        let mut h = std::io::stdout();
        loop {
            if verbose {
                write!(h, "." ).unwrap();
                h.flush().unwrap();
            }

            let out = next_page(&self.client, bucket, prefix, next_key, next_version).await?;

            next_key = out.next_key_marker.clone().map(String::from);
            next_version = out.next_version_id_marker.clone().map(String::from);
            acc.push(out);

            let records_so_far = acc.iter().map(|v|v.versions().len()).sum::<usize>();
            if records_so_far - prev_records_counter > 20000 {
                prev_records_counter = records_so_far;
                log::info!("Collected {} versioning records ...", formatter.format(records_so_far as f64));
            }

            if next_key.is_none() && next_version.is_none() {
                break;
            }
        }
        println!(" done");

        Ok(acc)
    }

    pub async fn purge_all_versions_of_everything(&self, bucket: &str, prefix: &str, verbose: bool) -> Result<()> {
        //TODO
        // self.assert_versioning_active().await?;
        let version_pages = self.get_versions(bucket, prefix, verbose).await?;

        for page in version_pages {
            let mut object_identifiers = Vec::new();

            let object_versions = page.versions.unwrap_or_default();
            let delete_markers = page.delete_markers.unwrap_or_default();

            let it = delete_markers.into_iter().map(|item| {
                ObjectIdentifier::builder()
                    .set_version_id(item.version_id)
                    .set_key(item.key)
                    .build()
                    .unwrap()
            });
            object_identifiers.extend(it);

            let it = object_versions.into_iter().map(|item| {
                ObjectIdentifier::builder()
                    .set_version_id(item.version_id)
                    .set_key(item.key)
                    .build()
                    .unwrap()
            });
            object_identifiers.extend(it);

            if !object_identifiers.is_empty() {
                log::info!("Deleting {} identifiers", object_identifiers.len());
                self.client
                    .delete_objects()
                    .bucket(bucket)
                    .delete(
                        Delete::builder()
                            .set_objects(Some(object_identifiers))
                            .build()
                            .unwrap(),
                    )
                    .send()
                    .await?;
            } else {
                log::info!("Nothing to delete")
            }
        }

        Ok(())
    }

    // async fn recursive_upload_helper(&self, de: &DirEntry, abs_path: &Path, bucket: &str, prefix: &str) -> Result<()> {
    //     let item_path = de.path();
    //     let stripped_path = item_path.strip_prefix(&abs_path).unwrap();
    //     let key = format!("{}/{}", prefix, stripped_path.to_string_lossy());
    //     //TODO don't restrict to string data
    //     let file_contents = std::fs::read_to_string(&item_path).unwrap();
    //     self.put_string_object(bucket, &key, &file_contents).await?;
    //     Ok(())
    // }

    // pub async fn put_string_recursive(&self, path: &Path, bucket: &str, prefix: &str) -> Result<()> {
    //     let abs_path = std::path::absolute(path)?;

    //     async fn visit_dirs(dir: &Path, abs_path: &Path, bucket: &str, prefix: &str) -> std::io::Result<()> {
    //         if dir.is_dir() {
    //             for entry in std::fs::read_dir(dir)? {
    //                 let entry = entry?;
    //                 let path = entry.path();
    //                 if path.is_dir() {
    //                     visit_dirs(&path, abs_path, bucket, prefix).await?;
    //                 } else {
    //                     self.recursive_upload_helper(&entry, &abs_path, bucket, prefix).await?;
    //                 }
    //             }
    //         }
    //         Ok(())
    //     }

    //     // let uploader = |de: &DirEntry| {
    //     //     let item_path = de.path();
    //     //     let stripped_path = item_path.strip_prefix(&abs_path).unwrap();
    //     //     let key = format!("{}/{}", prefix, stripped_path.to_string_lossy());
    //     //     //TODO don't restrict to string data
    //     //     let file_contents = std::fs::read_to_string(&item_path).unwrap();
    //     //     self.put_string_object(bucket, &key, &file_contents).await?;
    //     // };
    //     visit_dirs(&abs_path, &abs_path, bucket, prefix).await.unwrap();

    //     Ok(())
    // }

    // pub async fn put_string_object(&self, bucket: &str, key: &str, body: &str) -> Result<()> {
    //     let bytes = ByteStream::from(SdkBody::from(body.to_string()));

    //     let _ = self.client
    //         .put_object()
    //         .bucket(bucket)
    //         .key(key)
    //         .body(bytes)
    //         .send()
    //         .await?;

    //     Ok(())
    // }

    async fn get_utf8_object(&self, bucket: &str, key: &str) -> Result<String> {
            let bytes = self
                .client
                .get_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await?
                .body
                .try_next()
                .await?
                .ok_or_else(||eyre!("No bytes read from {}/{}", bucket, key))?;

            std::str::from_utf8(&bytes)
                .map(|t|t.to_string())
                .with_context(||"converting to utf8")
    }
}