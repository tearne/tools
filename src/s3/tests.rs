use std::env;

use aws_sdk_s3::Client;
use tokio::runtime::Runtime;


struct StorageTestHelper {
    bucket: String,
    prefix: String,
    delete_prefix_on_drop: bool,
    client: Client,
    runtime: Runtime,
}
impl StorageTestHelper {
    pub fn new(prefix: &str, delete_prefix_on_drop: bool) -> Self {
        if let Some(bucket) = env::var("TEST_BUCKET") {
            panic!(
                "You need to set the environment variable 'TEST_BUCKET' before running this test."
            );
        }

        // Expand bucket environment variables as appropriate
        let mut options = ExpandOptions::new();
        options.expansion_type = Some(ExpansionType::Unix);
        let bucket = envmnt::expand("${TEST_BUCKET}", Some(options));
        let prefix = envmnt::expand(prefix, Some(options));

        let runtime = Runtime::new().unwrap();

        let client = {
            let config = runtime.block_on(
                aws_config::from_env()
                    .region(Region::new("eu-west-1"))
                    .load(),
            );

            Client::new(&config)
        };

        let instance = StorageTestHelper {
            bucket,
            prefix,
            delete_prefix_on_drop,
            client,
            runtime,
        };

        //Delete anything that happens to already be in there
        instance.delete_prefix_recursively();

        instance
    }

    fn put_recursive(&self, proj_path: &str) {
        let abs_project_path = &test_data_path(proj_path);

        fn visit_dirs(dir: &Path, cb: &dyn Fn(&DirEntry)) -> std::io::Result<()> {
            if dir.is_dir() {
                for entry in std::fs::read_dir(dir)? {
                    let entry = entry?;
                    let path = entry.path();
                    if path.is_dir() {
                        visit_dirs(&path, cb)?;
                    } else {
                        cb(&entry);
                    }
                }
            }
            Ok(())
        }

        let prefix = Path::new(&self.prefix);

        let uploader = |de: &DirEntry| {
            let absolute_path = de.path();
            let stripped_path = absolute_path.strip_prefix(abs_project_path).unwrap();
            let object_name = prefix.join(stripped_path).to_string_lossy().into_owned();
            let file_contents = std::fs::read_to_string(&absolute_path).unwrap();
            self.put_object(&object_name, &file_contents);
        };
        visit_dirs(abs_project_path, &uploader).unwrap();
    }

    fn put_object(&self, key: &str, body: &str) {
        let bytes = ByteStream::from(Bytes::from(body.to_string()));

        self.runtime.block_on(async {
            self.client
                .put_object()
                .bucket(&self.bucket)
                .acl(ObjectCannedAcl::BucketOwnerFullControl)
                .key(key)
                .body(bytes)
                .send()
                .await
                .unwrap()
        });
    }

    fn get_object(&self, key: &str) -> String {
        self.runtime.block_on(async {
            let bytes = self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await
                .unwrap()
                .body
                .try_next()
                .await
                .unwrap()
                .unwrap();

            std::str::from_utf8(&bytes).unwrap().into()
        })
    }

    #[allow(dead_code)]
    fn list_objects_under(&self, sub_prefix: Option<&str>) -> Vec<Object> {
        let prefix = sub_prefix
            .map(|p| format!("{}/{}", self.prefix, p))
            .unwrap_or_else(|| self.prefix.clone());

        let response = self.runtime.block_on({
            self.client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix)
                .send()
        });

        let response = response.expect("Expected list objects response");
        assert!(response.continuation_token.is_none());
        response.contents.unwrap_or_default()
    }

    fn delete_prefix_recursively(&self) {
        if self.delete_prefix_on_drop {
            // if self
            //     .list_objects_under(None)
            //     .into_iter()
            //     .map(|o| ObjectIdentifier::builder().set_key(o.key).build())
            //     .next()
            //     .is_none()
            // {
            //     return;
            // }

            self.runtime.block_on(async {
                async fn next_page(
                    client: &Client,
                    bucket: &str,
                    prefix: &str,
                    next_key: Option<String>,
                    next_version: Option<String>,
                ) -> ABCDResult<ListObjectVersionsOutput> {
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

                let mut acc_version_pages: Vec<ListObjectVersionsOutput> = Vec::new();

                loop {
                    let out = next_page(
                        &self.client,
                        &self.bucket,
                        &self.prefix,
                        next_key,
                        next_version,
                    )
                    .await
                    .unwrap();

                    next_key = out.next_key_marker.clone().map(String::from);
                    next_version = out.next_version_id_marker.clone().map(String::from);

                    acc_version_pages.push(out);

                    if next_key.is_none() && next_version.is_none() {
                        break;
                    }
                }

                for page in acc_version_pages {
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
                        self.client
                            .delete_objects()
                            .bucket(&self.bucket)
                            .delete(
                                Delete::builder()
                                    .set_objects(Some(object_identifiers))
                                    .build(),
                            )
                            .send()
                            .await
                            .expect("delete objects failed");
                    } else {
                        log::info!("Nothing to delete")
                    }
                }

                // let _remaining = self
                //     .client
                //     .list_objects_v2()
                //     .bucket(&self.bucket)
                //     .prefix(&self.prefix)
                //     .send()
                //     .await
                //     .unwrap();
            })
        }
    }
}
impl Drop for StorageTestHelper {
    fn drop(&mut self) {
        if self.delete_prefix_on_drop {
            self.delete_prefix_recursively();
        }
    }
}