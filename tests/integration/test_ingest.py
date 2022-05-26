from data_transformations.citibike import ingest


def test_should_sanitize_column_names(helpers, spark_session) -> None:
    given_ingest_folder, given_transform_folder = helpers.create_input_and_output_folders()
    input_csv_path = given_ingest_folder + 'input.csv'
    csv_content = [
        ['first_field', 'field with space', ' fieldWithOuterSpaces '],
        ['3', '4', '1'],
        ['1', '5', '2'],
    ]
    helpers.write_csv_file(input_csv_path, csv_content)
    ingest.run(spark_session, input_csv_path, given_transform_folder)

    actual = spark_session.read.parquet(given_transform_folder)
    expected = spark_session.createDataFrame(
        [
            ['3', '4', '1'],
            ['1', '5', '2']
        ],
        ['first_field', 'field_with_space', '_fieldWithOuterSpaces_']
    )

    assert expected.collect() == actual.collect()
