use aws_sdk_s3 as s3;

#[::tokio::main]
async fn main() -> Result<(), s3::Error> {
    let config = aws_config::load_from_env().await;
    let client = aws_sdk_s3::Client::new(&config);

    // ... make some calls with the client

    Ok(())
}

pub fn purge_all_versions_of_everything_in_prefix(&self) -> ABCDResult<()> {
    self.handle.block_on(async {
        self.assert_versioning_active().await?;
        let version_pages = self.get_versions(&self.prefix).await?;

        for page in version_pages {
            let mut object_identifiers = Vec::new();

            let object_versions = page.versions.unwrap_or_default();
            let delete_markers = page.delete_markers.unwrap_or_default();

            let it = delete_markers.into_iter().map(|item| {
                ObjectIdentifier::builder()
                    .set_version_id(item.version_id)
                    .set_key(item.key)
                    .build()
            });
            object_identifiers.extend(it);

            let it = object_versions.into_iter().map(|item| {
                ObjectIdentifier::builder()
                    .set_version_id(item.version_id)
                    .set_key(item.key)
                    .build()
            });
            object_identifiers.extend(it);

            if !object_identifiers.is_empty() {
                log::info!("Deleting {} identifiers", object_identifiers.len());
                self.client
                    .delete_objects()
                    .bucket(&self.bucket)
                    .delete(
                        Delete::builder()
                            .set_objects(Some(object_identifiers))
                            .build(),
                    )
                    .send()
                    .await?;
            } else {
                log::info!("Nothing to delete")
            }
        }

        ABCDResult::Ok(())
    })
}

async fn assert_versioning_active(&self) -> ABCDResult<()> {
    let enabled = self
        .client
        .get_bucket_versioning()
        .bucket(&self.bucket) 
        .send()
        .await?
        .status
        .map(|s| s == BucketVersioningStatus::Enabled)
        .unwrap_or(false);
    if enabled {
        Ok(())
    } else {
        Err(ABCDErr::InfrastructureError(
            "S3 bucket versioning must be enabled".into(),
        ))
    }
}