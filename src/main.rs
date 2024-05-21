use umbra::{MetadataJson, Result, write_to_file};
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    // let file = "data/2023-11-10-09-49-26_UMBRA-05_METADATA.json";
    let file = "data/2023-12-30-12-25-44_UMBRA-04_METADATA.json";
    let mut metada_json = MetadataJson::new(file).await?;
    // println!("{:?}", metada_json);

    let ctx = SessionContext::new();
    let metada_json_df = metada_json.to_df(ctx).await?;
    // metada_json_df.show().await?;
    write_to_file(metada_json_df, "data/foo.parquet").await?;

    Ok(())
}
