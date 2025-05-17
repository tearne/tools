use aws_sdk_s3::{operation::{list_object_versions::ListObjectVersionsOutput, list_objects_v2::ListObjectsV2Output}, types::{BucketVersioningStatus, Object}, Client};
use tokio::runtime::{Handle, Runtime};
use color_eyre::{eyre::OptionExt, Result};

// #[::tokio::main]
fn main() -> Result<()> {
    let runtime = Runtime::new().unwrap();
    let handle = runtime.handle().clone();

    let client = {
            let config = handle.block_on(
                aws_config::load_from_env()
            );
            Client::new(&config)
        };


    let s3 = S3Wrapper{
        handle,
        client
    };

    Ok(())
}

struct S3Wrapper {
    handle: Handle,
    client: Client
}

impl S3Wrapper {
    async fn list_objects_v2(&self, bucket: &str, prefix: &str) -> Result<Vec<Object>> {
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

    async fn is_versioning_enabled(&self, bucket: &str) -> Result<bool> {
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

    async fn get_versions(&self, bucket: &str, prefix: &str) -> Result<Vec<ListObjectVersionsOutput>> {
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

        loop {
            let out = next_page(&self.client, bucket, prefix, next_key, next_version).await?;

            next_key = out.next_key_marker.clone().map(String::from);
            next_version = out.next_version_id_marker.clone().map(String::from);

            acc.push(out);

            log::debug!("Found {} page(s) of version identifiers.", acc.len());

            if next_key.is_none() && next_version.is_none() {
                break;
            }
        }

        Ok(acc)
    }
}
