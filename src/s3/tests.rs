use std::{env, fs::DirEntry, path::{Path, PathBuf}};

use aws_sdk_s3::{operation::list_object_versions::ListObjectVersionsOutput, primitives::{ByteStream, SdkBody}, types::{Delete, Object, ObjectIdentifier}, Client};
use tokio::runtime::Runtime;
use color_eyre::Result;

use super::wrapper::S3Wrapper;


// Helper to identify project dir
fn project_path(proj_path: &str) -> PathBuf {
    let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    d.push(proj_path);
    d
}

struct StorageTestHelper {
    bucket: String,
    prefix: String,
    delete_prefix_on_drop: bool,
    s3_wrapper: S3Wrapper,
}
impl StorageTestHelper {
    pub fn new(prefix: &str, delete_prefix_on_drop: bool) -> Self {
        let bucket = 
            if let Ok(bucket) = env::var("TEST_BUCKET") {
                bucket
            } else {
                panic!(
                    "You need to set the environment variable 'TEST_BUCKET' before running this test.  Anything in there will be deleted during tests."
                );
            };

               
        let s3_wrapper = {
            let runtime = Runtime::new().unwrap();
            let client = {
                let config = runtime.block_on(async {aws_config::load_from_env().await});
                Client::new(&config)
            };
            
            S3Wrapper{
                handle: runtime.handle().clone(),
                client,
            }
        };

        //Delete anything that happens to already be in there
        s3_wrapper.purge_all_versions_of_everything(&bucket, &prefix);

        let instance = StorageTestHelper {
            bucket,
            prefix: prefix.into(),
            delete_prefix_on_drop,
            s3_wrapper
        };

        instance
    }

}
impl Drop for StorageTestHelper {
    fn drop(&mut self) {
        if self.delete_prefix_on_drop {
            self.s3_wrapper.purge_all_versions_of_everything(&self.bucket, &self.prefix);
        }
    }
}