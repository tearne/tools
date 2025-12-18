use std::{borrow::Borrow, collections::HashSet, fmt::Display};

use aws_sdk_s3::types::{Object, ObjectVersion};
use bytesize::ByteSize;
use serde::Serialize;
use color_eyre::Result;

use super::{types::S3Location, wrapper::S3Wrapper};


#[derive(Debug, PartialEq, Eq)]
pub struct Stats {
    pub num_objects: usize,
    pub size: ByteSize,
}
impl Stats {
    pub fn from_object_versions<T: Borrow<ObjectVersion>>(items: &[T]) -> Self {
        let size = ByteSize::b(items.iter().map(|o|o.borrow().size.expect("Object has no size.")).sum::<i64>() as u64);
        Stats {
            num_objects: items.len(),
            size,
        }
    }

    pub fn from_objects<T: Borrow<Object>>(items: &[T]) -> Self {
        let size = ByteSize::b(items.iter().map(|o|o.borrow().size.expect("Object has no size.")).sum::<i64>() as u64);
        Stats {
            num_objects: items.len(),
            size,
        }
    }
}

#[derive(Debug)]
pub struct SizeReport {
    pub url: String,
    pub total: Stats,
    pub versions: Option<VersionData>,
}
impl AsRef<SizeReport> for SizeReport {
    fn as_ref(&self) -> &SizeReport {
        self
    }
}
impl Display for SizeReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(
            format_args!(
                "{}:\n  {} (current obj: {}, current vers: {}, orphaned vers: {})", 
                self.url, 
                self.total.size, 
                self.versions.as_ref().expect("No versioning data for current obj.").current_objects.size, 
                self.versions.as_ref().expect("No versioning data for current vers.").current_obj_vers.size, 
                self.versions.as_ref().expect("No versioning data for orphaned vers.").orphaned_vers.size
            )
        )
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct VersionData {
    pub current_objects: Stats,
    pub current_obj_vers: Stats,
    pub orphaned_vers: Stats,
}

#[derive(Debug, Serialize)]
pub struct CSVSizeReport {
    url: String,
    
    total_human: String,
    total_b: u64,
    total_qty: usize,
    
    versioning_active: bool,

    current_obj_human: String,
    current_ver_human: String,
    orphan_ver_human: String,

    current_obj_b: u64,
    current_ver_b: u64,
    orphan_ver_b: u64,

    current_ver_qty: usize,
    current_obj_qty: usize,
    orphan_ver_qty: usize,
}
impl<T: AsRef<SizeReport>> From<T> for CSVSizeReport{
    fn from(value: T) -> CSVSizeReport {
        let report = value.as_ref();
        CSVSizeReport { 
            url: report.url.clone(), 
            total_human: report.total.size.to_string(), 
            total_b: report.total.size.0, 
            total_qty: report.total.num_objects, 
            versioning_active: report.versions.is_some(),

            current_obj_human: report.versions.as_ref().map(|v|v.current_objects.size.to_string()).unwrap_or_default(), 
            current_ver_human: report.versions.as_ref().map(|v|v.current_obj_vers.size.to_string()).unwrap_or_default(), 
            orphan_ver_human: report.versions.as_ref().map(|v|v.orphaned_vers.size.to_string()).unwrap_or_default(), 

            current_obj_b: report.versions.as_ref().map(|v|v.current_objects.size.0).unwrap_or_default(), 
            current_ver_b: report.versions.as_ref().map(|v|v.current_obj_vers.size.0).unwrap_or_default(), 
            orphan_ver_b: report.versions.as_ref().map(|v|v.orphaned_vers.size.0).unwrap_or_default(), 

            current_obj_qty: report.versions.as_ref().map(|v|v.current_objects.num_objects).unwrap_or_default(), 
            current_ver_qty: report.versions.as_ref().map(|v|v.current_obj_vers.num_objects).unwrap_or_default(), 
            orphan_ver_qty: report.versions.as_ref().map(|v|v.orphaned_vers.num_objects).unwrap_or_default(), 
        }
    }
}

pub async fn build_size_report(s3_location: &S3Location, s3: &S3Wrapper, verbose: bool) -> Result<SizeReport> {
    if s3.is_versioning_enabled(&s3_location.bucket).await? {
        let versions = s3.get_object_versions(&s3_location.bucket, &s3_location.prefix, verbose).await?;
        
        let total = Stats::from_object_versions(&versions);
        
        let current: Vec<_> = versions.iter().filter(|t|{
            t.is_latest.unwrap_or(false)
        }).collect();
        let current_object_keys: HashSet<String> = current.iter().map(|t|{
            t.key.as_ref().expect("S3 API issue No key for object.").clone()
        }).collect();
        let current_objects = Stats::from_object_versions(&current);

        let (current, orphaned): (Vec<_>, Vec<_>) = versions.iter()
            .filter(|t|!t.is_latest.expect("S3 API issue is_latest unpopulated."))
            .partition(|t|{
                t.key().map(|k|current_object_keys.contains(k)).expect("S3 API issue No key for object.")
            });

        let current_obj_vers = Stats::from_object_versions(&current);
        let orphaned_vers = Stats::from_object_versions(&orphaned);

        let report = SizeReport {
            url: s3_location.to_string(),
            total,
            versions: Some(VersionData{
                current_objects,
                current_obj_vers,
                orphaned_vers,
            })
        };

        Ok(report)
    } else {
        log::warn!("Versioning is NOT active on {}", s3_location);
        let objects = s3.list_objects_v2(&s3_location.bucket, &s3_location.prefix).await?;
        let stats = Stats::from_objects(&objects);

        Ok(SizeReport{
            url: s3_location.to_string(),
            total: stats,
            versions: None,
        })

    }
}