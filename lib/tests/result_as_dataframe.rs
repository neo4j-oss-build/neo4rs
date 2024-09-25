#![cfg(feature = "polars_v0_43")]

use neo4rs::query;
use polars::prelude::DataType;

mod container;

#[tokio::test]
async fn result_as_dataframe() {
    let neo4j = container::Neo4jContainer::new().await;
    let graph = neo4j.graph();

    let result = graph
        .execute(query(
            r#"
            WITH
                [42, 84, 420, 1337] AS integers,
                [42.0, 84.21, 420.0, 1337.37] AS floats,
                [TRUE, FALSE, NULL, 1] AS booleans,
                ['hello', ',', 'world', '!'] AS strings
            UNWIND range(0, 3) AS i
            RETURN integers[i] AS integers, floats[i] AS floats, booleans[i] AS booleans, strings[i] AS strings
            "#,
        ))
        .await
        .unwrap();

    let df = result.into_dataframe().await.unwrap();
    #[cfg(feature = "unstable-result-summary")]
    let df = df.0;

    println!("{:?}", df);

    assert_eq!(
        df.get_column_names(),
        ["integers", "floats", "booleans", "strings"]
    );
    assert_eq!(df.height(), 4);
    assert_eq!(df.width(), 4);

    let integers = df.column("integers").unwrap();
    assert_eq!(integers.dtype(), &DataType::Int64);
    let integers = integers.i64().unwrap().iter().flatten().collect::<Vec<_>>();
    assert_eq!(integers, [42, 84, 420, 1337]);

    let floats = df.column("floats").unwrap();
    assert_eq!(floats.dtype(), &DataType::Float64);
    let floats = floats.f64().unwrap().iter().flatten().collect::<Vec<_>>();
    assert_eq!(floats, [42.0, 84.21, 420.0, 1337.37]);

    let booleans = df.column("booleans").unwrap();
    assert_eq!(booleans.dtype(), &DataType::Boolean);
    let booleans = booleans
        .bool()
        .unwrap()
        .iter()
        .map(|a| a.unwrap_or_default())
        .collect::<Vec<_>>();
    assert_eq!(booleans, [true, false, false, true]);

    let strings = df.column("strings").unwrap();
    assert_eq!(strings.dtype(), &DataType::String);
    let strings = strings.str().unwrap().iter().flatten().collect::<Vec<_>>();
    assert_eq!(strings, ["hello", ",", "world", "!"]);
}
