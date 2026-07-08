"""
GX Core Workflow - Pandas and Ephemeral Data Context

Demonstrates:
- Import Required Libraries
- Create GX Context (Ephemeral)
- Create DataSource for Pandas DataFrame
- Create Data Asset
- Create Batch Definition
- Create Expectations
- Create Expectations Suite and Add expectations
- Create Validation Definition (Expectation Suite, Batch Definition)
- Load Dataset in Pandas Dataframe
- Define Batch Parameters
- Run Validation Definition (Batch Parameters)
- Print Result
"""

# Import Libraries
import great_expectations as gx
import pandas as pd

# Creating GX Context
context = gx.get_context(mode="ephemeral")

# Creating DataSource
data_source_name = 'temperature_data'
data_source = context.data_sources.add_pandas(name=data_source_name)

# Creating DataAsset
data_asset_name = 'temperature_entity_asset'
data_asset = data_source.add_dataframe_asset(name=data_asset_name)

# Creating BatchDefinition
batch_definition_name = 'full_batch'
batch_definition = data_asset.add_batch_definition_whole_dataframe(batch_definition_name)

# Creating Expectations
expectation_temp = gx.expectations.ExpectColumnValuesToBeBetween(
    column="Temperature",
    max_value=45,
    min_value=10
)

expectation_city = gx.expectations.ExpectColumnDistinctValuesToBeInSet(
    column="City",
    value_set=["Mumbai", "Delhi"]
)

# Creating ExpectationSuite and adding Expectations
expectation_suite_name = "temperature_data_suite"
expectation_suite_ref = gx.ExpectationSuite(name=expectation_suite_name)
expectation_suite = context.suites.add(expectation_suite_ref)

# Adding Expectation to ExpectationSuite
expectation_suite.add_expectation(expectation_temp)
expectation_suite.add_expectation(expectation_city)

# Creating Validation Definition
validation_def_name = "temperature_data_validation"
validation_definition_ref = gx.ValidationDefinition(
    data=batch_definition,
    suite=expectation_suite,
    name=validation_def_name
)

validation_definition = context.validation_definitions.add(validation_definition_ref)

# Reading Data in Pandas DataFrame
data_df = pd.read_csv('/Users/naveenkumarreddyreddivari/Git_2026_personal/2026_pyspark_demo/2026_pyspark_demo/src/great_expectations/temperature.csv')
print("\nData Preview:")
print(data_df.head())

# Creating BatchParameter and running Validation
batch_parameters = {"dataframe": data_df}

print("\nRunning validation...")
validation_result = validation_definition.run(batch_parameters=batch_parameters)

# Displaying Validation Result
print("\nValidation Result:")
print(validation_result)
