use std::{borrow::Borrow, fmt::Display};

use aws_sdk_s3::types::{Object, ObjectVersion};
use bytesize::ByteSize;
use regex::Regex;
use color_eyre::{Result, eyre::eyre};


pub struct S3Path{
    pub bucket: String,
    pub prefix: String,
}
impl S3Path{
    pub fn parse(url: &str) -> Result<S3Path>{
        let s3_path_re = Regex::new(
                // https://regex101.com/r/wAmOQU/1
                r#"^([Ss]3://)?(?P<bucket>[^/]*)(?P<prefix>[\w/.-]*)$"#,
            )?;

            let captures = s3_path_re
                .captures(url)
                .ok_or_else(|| eyre!("No regex matches"))?;
            let bucket = captures.name("bucket").unwrap().as_str().to_string();
            let prefix = captures
                .name("prefix")
                .unwrap()
                .as_str();
            let prefix = prefix.strip_prefix('/').unwrap_or(prefix);
            let prefix = prefix.strip_suffix('/').unwrap_or(prefix).to_string();

        Ok(S3Path{ bucket, prefix })
    }
}
impl Display for S3Path {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("s3://{}/{}", self.bucket, self.prefix))
    }
}

#[derive(Debug)]
pub struct Stats {
    pub num_objects: usize,
    pub size: ByteSize,
}
impl Stats {
    pub fn from_object_versions<T: Borrow<ObjectVersion>>(items: &[T]) -> Self {
        let size = ByteSize::b(items.iter().map(|o|o.borrow().size.unwrap()).sum::<i64>() as u64);
        Stats {
            num_objects: items.len(),
            size,
        }
    }

    pub fn from_objects<T: Borrow<Object>>(items: &[T]) -> Self {
        let size = ByteSize::b(items.iter().map(|o|o.borrow().size.unwrap()).sum::<i64>() as u64);
        Stats {
            num_objects: items.len(),
            size,
        }
    }
}