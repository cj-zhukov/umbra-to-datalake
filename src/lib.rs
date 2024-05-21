pub type Result<T> = core::result::Result<T, Error>;
pub type Error = Box<dyn std::error::Error>;

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, StringArray, Float64Array};
use datafusion::arrow::datatypes::{Schema, Field, DataType};
use datafusion::prelude::*;
use parquet::arrow::AsyncArrowWriter;
use serde::Deserialize;
use serde_json::Value;
use tokio::fs;
use tokio_stream::StreamExt;

#[derive(Deserialize, Debug)]
pub struct MetadataJson {
    pub version: Option<String>,
    pub vendor: Option<String>, 
    #[serde(rename = "imagingMode")]
    pub imaging_mode: Option<String>, 
    #[serde(rename = "orderType")]
    pub order_type: Option<String>, 
    #[serde(rename = "productSku")]
    pub product_sku: Option<String>, 
    #[serde(rename = "baseIpr")]
    pub base_ipr: Option<f64>, 
    #[serde(rename = "targetIpr")]
    pub target_ipr: Option<f64>, 
    #[serde(rename = "umbraSatelliteName")]
    pub umbra_satellite_name: Option<String>, 
    pub collects: Option<Vec<HashMap<String, Value>>>, 
    #[serde(rename = "derivedProducts")]
    pub derived_products: Option<HashMap<String, Value>>,
}

impl MetadataJson {
    pub async fn new(file_path: &str) -> Result<Self> {
        let contents = fs::read_to_string(file_path).await?;
        let metadata_json: Self = serde_json::from_str(contents.as_str())?;

        Ok(metadata_json)
    }
}

impl MetadataJson {
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new("version", DataType::Utf8, true),
            Field::new("vendor", DataType::Utf8, true),
            Field::new("imaging_mode", DataType::Utf8, true),
            Field::new("order_type", DataType::Utf8, true),
            Field::new("product_sku", DataType::Utf8, true),
            Field::new("base_ipr", DataType::Float64, true),
            Field::new("target_ipr", DataType::Float64, true),
            Field::new("umbra_satellite_name", DataType::Utf8, true),
            Field::new("collects", DataType::Utf8, true),
            Field::new("derived_products", DataType::Utf8, true),
        ])
    }

    pub async fn to_df(&mut self, ctx: SessionContext) -> Result<DataFrame> {
        let schema = Self::schema();

        let collects = match &mut self.collects {
            None => None,
            Some(vals) => {
                let json: String = serde_json::to_string(&vals)?;
                Some(json)
            }
        };

        let derived_products = match &mut self.derived_products {
            None => None, 
            Some(vals) => {
                let json: String = serde_json::to_string(&vals)?;
                Some(json)
            }
        };

        let batch = RecordBatch::try_new(
            schema.into(),
            vec![
                Arc::new(StringArray::from(vec![self.version.clone()])),
                Arc::new(StringArray::from(vec![self.vendor.clone()])),
                Arc::new(StringArray::from(vec![self.imaging_mode.clone()])),
                Arc::new(StringArray::from(vec![self.order_type.clone()])),
                Arc::new(StringArray::from(vec![self.product_sku.clone()])),
                Arc::new(Float64Array::from(vec![self.base_ipr])),
                Arc::new(Float64Array::from(vec![self.target_ipr])),
                Arc::new(StringArray::from(vec![self.umbra_satellite_name.clone()])),
                Arc::new(StringArray::from(vec![collects])),
                Arc::new(StringArray::from(vec![derived_products])),
            ],
        )?;
        let df = ctx.read_batch(batch)?;

        Ok(df)
    }
}

pub async fn write_to_file(df: DataFrame, file_path: &str) -> Result<()> {
    let mut buf = vec![];
    let schema = Schema::from(df.clone().schema());
    let mut stream = df.execute_stream().await?;
    let mut writer = AsyncArrowWriter::try_new(&mut buf, schema.into(), None)?;
    while let Some(batch) = stream.next().await.transpose()? {
        writer.write(&batch).await?;
    }
    writer.close().await?;
    let mut file = std::fs::File::create(file_path)?;
    file.write_all(&buf)?;

    Ok(())
}