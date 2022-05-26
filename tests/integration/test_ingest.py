from source.jobs.citibike.ingestion import citibike_ingest


def test_citibike_ingest_sanitization_and_content(helpers, spark_session) -> None:
    given_ingest_folder, given_transform_folder = helpers.create_input_and_output_folders()
    input_csv_path = given_ingest_folder + '/input.csv'
    output_parquet_path = given_transform_folder + '/output.parquet'
    csv_content = [
        ['first_field', 'field with space', ' fieldWithOuterSpaces '],
        ['3', '4', '1'],
        ['1', '5', '2'],
    ]
    helpers.write_csv_file(input_csv_path, csv_content)
    citibike_ingest.run(spark_session, input_csv_path, output_parquet_path, header=True)

    actual = spark_session.read.parquet(output_parquet_path)
    expected = spark_session.createDataFrame(
        [
            ['3', '4', '1'],
            ['1', '5', '2']
        ],
        ['first_field', 'field_with_space', '_fieldWithOuterSpaces_']
    )

    assert expected.collect() == actual.collect()
