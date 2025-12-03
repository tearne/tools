use std::io::Write;

use aws_sdk_s3::{operation::{list_object_versions::ListObjectVersionsOutput, list_objects_v2::ListObjectsV2Output}, types::{BucketVersioningStatus, Delete, Object, ObjectIdentifier, ObjectVersion}, Client};
use human_format::Formatter;

use color_eyre::{Result, eyre::{Context, OptionExt}};


pub struct S3Wrapper {
    pub client: Client
}

impl S3Wrapper {
    pub async fn get_object_versions(&self, bucket: &str, prefix: &str, verbose: bool) -> Result<Vec<ObjectVersion>> {
        let pages = self.get_versions(bucket, prefix, verbose).await?;
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
                write!(h, "." )?;
                h.flush()?;
            }

            let out = next_page(&self.client, bucket, prefix, next_key, next_version).await?;

            next_key = out.next_key_marker.clone();
            next_version = out.next_version_id_marker.clone();
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
                .build().expect("Build error for delete markers.")
            });
            object_identifiers.extend(it);

            let it = object_versions.into_iter().map(|item| {
                ObjectIdentifier::builder()
                    .set_version_id(item.version_id)
                    .set_key(item.key)
                    .build()
                    .expect("Build error for object versions.")
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
                            .expect("Build error for delete builder."),
                    )
                    .send()
                    .await?;
            } else {
                log::info!("Nothing to delete")
            }
        }

        Ok(())
    }
}
