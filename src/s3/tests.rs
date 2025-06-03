use std::{env, path::{Path, PathBuf}, process::Command};

use aws_sdk_s3::{Client};
use bytesize::ByteSize;
use tokio::runtime::{Handle, Runtime};
use color_eyre::{eyre::WrapErr, Result};

use crate::s3::size::{Stats, VersionData};

use super::{size::build_size_report, types::S3Location, wrapper::S3Wrapper};


struct StorageTestHelper {
    s3_location: S3Location,
    prefix: String,
    delete_prefix_on_drop: bool,
    s3_wrapper: S3Wrapper,
    runtime: Runtime,
}
impl StorageTestHelper {
    pub fn new(prefix: &str, delete_prefix_on_drop: bool) -> Result<Self> {
        let bucket = 
            if let Ok(bucket) = env::var("TEST_BUCKET") {
                bucket
            } else {
                panic!(
                    "You need to set the environment variable 'TEST_BUCKET' before running this test.  Anything in there will be deleted during tests."
                );
            };


        let runtime = Runtime::new().unwrap();   
        let s3_wrapper = {
            let client = {
                let config = runtime.block_on(async {aws_config::load_from_env().await});
                Client::new(&config)
            };
            
            S3Wrapper{
                client,
            }
        };

        let instance = StorageTestHelper {
            s3_location: S3Location { bucket, prefix: prefix.to_string() },
            prefix: prefix.into(),
            delete_prefix_on_drop,
            s3_wrapper,
            runtime,
        };

        instance.purge_storage()?;

        Ok(instance)
    }

    fn purge_storage(&self) -> Result<()> {
        println!("Purging storage: {}", self.s3_location);
        self.runtime.block_on(
            self.s3_wrapper.purge_all_versions_of_everything(
                &self.s3_location.bucket, 
                &self.s3_location.prefix,
                false
            )
        )
    }

    fn sync_test_data<P: AsRef<Path>>(&self, path: &P) -> Result<()> {
        let source_path = path.as_ref().to_string_lossy().into_owned();

        let program = "aws";
        let args = [
                "s3", 
                "sync",
                "--delete",
                &source_path,
                &self.s3_location.to_string()
            ];

        let _ = Command::new(program)
            .args(args)
            .spawn()?
            .wait()
            .with_context(|| {
                format!(
                    "Failed to run: {} {}",
                    program,
                    args.join(" ")
                )
            })?;

        Ok(())
    }

    // // Helper to identify project dir
    // fn project_path(path: &str) -> PathBuf {
    //     let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    //     d.push(path);
    //     d
    // }
}
impl Drop for StorageTestHelper {
    fn drop(&mut self) {
        if self.delete_prefix_on_drop {
            let _ = self.purge_storage();
        }
    }
}


// TEST_BUCKET=my-bucket cargo test --package tools --lib -- s3::tests --show-output
#[test]
fn test_basic_upload() -> Result<()> {
    let helper = StorageTestHelper::new(
        "test_basic_upload", 
        false
    )?;

    helper.sync_test_data(&"resources/test/s3/data_v1")?;

    let report = helper.runtime.block_on(async {
        build_size_report(
            &helper.s3_location,
            &helper.s3_wrapper,
            false
        ).await
    })?;

    let expected = Stats{
        num_objects: 2,
        size: ByteSize::b(38 + 78),
    };

    assert_eq!(expected, report.total);
    
    Ok(())
}

#[test]
fn test_with_versions() -> Result<()> {
    let helper = StorageTestHelper::new(
        "test_with_versions", 
        false
    )?;

    helper.sync_test_data(&"resources/test/s3/data_v1")?;
    helper.sync_test_data(&"resources/test/s3/data_v2")?;

    let report = helper.runtime.block_on(async {
        build_size_report(
            &helper.s3_location,
            &helper.s3_wrapper,
            false
        ).await
    })?;

    let expected_versions = VersionData {
        current_objects: Stats { num_objects: 1, size: ByteSize(152) },
        current_obj_vers: Stats { num_objects: 1, size: ByteSize(78) },
        orphaned_vers: Stats { num_objects: 1, size: ByteSize(38) },
    };

    assert_eq!(expected_versions, report.versions.unwrap());
    
    Ok(())
}