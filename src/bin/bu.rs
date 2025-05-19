use std::collections::HashSet;

use aws_sdk_s3::{operation::{get_object_attributes::GetObjectAttributesOutput, list_object_versions::ListObjectVersionsOutput, list_objects_v2::ListObjectsV2Output}, types::{BucketVersioningStatus, Delete, Object, ObjectIdentifier, ObjectVersion}, Client};
use bytesize::ByteSize;
use clap::Parser;
use tokio::runtime::{Handle, Runtime};
use color_eyre::{eyre::OptionExt, Result};

#[derive(Parser)]
#[command(version, about)]
/// Run a command, monitoring CPU and RAM usage at regular intervals and saving to a CSV file.
struct Cli {
    /// Bucket
    #[structopt(short, long)]
    bucket: String,

    /// Prefix
    #[structopt(short, long, default_value="")]
    prefix: String,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

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

    s3.do_stuff(&cli.bucket, &cli.prefix);

    Ok(())
}

struct S3Wrapper {
    handle: Handle,
    client: Client
}

impl S3Wrapper {
    pub fn do_stuff(&self,bucket: &str, prefix: &str) -> Result<()> {
        self.handle.block_on(async {
            println!("{bucket}/{prefix}");

            let objects = self.list_objects_v2(bucket, prefix).await.unwrap();
            let size = ByteSize::b(objects.iter().map(|o|o.size.unwrap()).sum::<i64>() as u64);
            println!(" * {} across {} objects", size, objects.len());

            if self.is_versioning_enabled(bucket).await.unwrap() {
                let versions = self.get_object_versions(bucket, prefix).await.unwrap();
                
                // let count = versions.iter().filter(|t|t.is_latest().unwrap_or(false)).count();
                let size =  ByteSize::b(versions.iter().map(|t|t.size.unwrap()).sum::<i64>() as u64);
                println!("Total usage is {} ({} objects) of which:", size, versions.len());
                
                let current: Vec<_> = versions.iter().filter(|t|{
                    t.is_latest.unwrap_or(false)
                }).collect();
                let current_object_keys: HashSet<String> = current.iter().map(|t|{
                    t.key.as_ref().unwrap().clone()
                }).collect();
                let size = 
                ByteSize::b(current.iter().map(|o|o.size.unwrap()).sum::<i64>() as u64); 
                println!(" * {} is current ({} objects)", size, current_object_keys.len());


                println!(" * ?? are old versions of current objects (?? versions across ?? objects)");


                let orphans: Vec<_> = versions.iter().filter(|t|{
                    let is_current_version = t.is_latest.unwrap_or(false);
                    let is_orphan = !t.key().map(|k|current_object_keys.contains(k)).unwrap_or(false);
                    !is_current_version && is_orphan
                }).collect();
                let orphan_keys_unique = orphans.iter().map(|t|t.key().unwrap()).collect::<HashSet<_>>().len();
                let size = ByteSize::b(orphans.iter().map(|o|o.size.unwrap()).sum::<i64>() as u64); 
                println!(" * {} are versions of deleted objects ({} versions across {} objects)", size, orphans.len(), orphan_keys_unique);
            } else {
                println!(" * Versioning is NOT active");
                let objects = self.list_objects_v2(bucket, prefix).await.unwrap();
                let size = ByteSize::b(objects.iter().map(|o|o.size.unwrap()).sum::<i64>() as u64);
                println!(" * {} across {} objects", size, objects.len());
            }
            

            
        });
        
        Ok(())
    }

    async fn get_object_versions(&self, bucket: &str, prefix: &str) -> Result<Vec<ObjectVersion>> {
        let pages = self.get_versions(bucket, prefix).await.unwrap();
        let object_versions: Vec<ObjectVersion> = pages.into_iter()
            .flat_map(|page|
                page.versions.unwrap_or_default())
            .collect();


        // let mut handles = Vec::new();
    
        // for object_identifier in object_identifiers {
        //     let obj_identifier = object_identifier.unwrap();
        //     handles.push(tokio::spawn(self.client
        //         .get_object_attributes()
        //         .bucket(bucket)
        //         .key(obj_identifier.key)
        //         .version_id(obj_identifier.version_id.unwrap())
        //         .send()));
        // }

        // //TODO replace with tokio::task::JoinSet
        // let mut out = Vec::new();
        // for handle in handles {
        //     out.push(handle.await.unwrap())
        // }

        // let t: Result<Vec<GetObjectAttributesOutput>,_> = out.into_iter().collect();
        
        // let t: Vec<_> = t.unwrap().iter().map(|t|{
        //     t.object_size.unwrap()
        // }).collect();

        Ok(object_versions)
    }

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
