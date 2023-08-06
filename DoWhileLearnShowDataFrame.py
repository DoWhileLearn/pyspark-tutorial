from pyspark.sql import SparkSession
from pyspark.sql.functions import substring
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from tabulate import tabulate


class DoWhileLearnShowDataFrame:
    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()
        self.employee_df = self.create_dataframe()
  

    def create_dataframe(self):
        # Define the schema for the employee table
        schema = StructType([
            StructField("employee_id", IntegerType(), True),
            StructField("employee_name", StringType(), True),
            StructField("department", StringType(), True)
        ])

        # Dummy data for the employee table
        data = [
            (1, "John Doe", "Sales"),
            (2, "Jane Smith", "Marketing"),
            (3, "Michael Johnson", "HR"),
            (4, "Mary Williams", "Finance"),
            (5, "David Jones", "IT"),
           (6, "Rajesh", "Sales"),
           (7, "Ramesh", "Marketing"),
           (8, "Suresh", "HR"),
           (9, "Peter", "Finance"),
           (10, "Paul", "IT")
            
        ]

        # Create a DataFrame from the dummy data and schema
        df = self.spark.createDataFrame(data, schema)
        return df
    

    def display_employee_df(self):
        # Display the DataFrame
        print("Displaying the DataFrame")
        self.employee_df.show()

    def display_specific_columns(self, *columns):
        # Display the DataFrame with only the specified columns
        print("Displaying the DataFrame with only the specified columns")
        self.employee_df.select(*columns).show()
        

    def display_first_n_rows(self, n):
        # Display the first n rows of the DataFrame
        rows = self.employee_df.head(n)
        headers = rows[0].asDict().keys()
        data = [row.asDict().values() for row in rows]

        print("Displaying the first n rows of the DataFrame:")
        print(tabulate(data, headers=headers, tablefmt="grid"))


    def display_last_n_rows(self, n):
        # Display the last n rows of the DataFrame
        rows = self.employee_df.tail(n)
        headers = rows[0].asDict().keys()
        data = [row.asDict().values() for row in rows]

        print("Displaying the last n rows of the DataFrame:")
        print(tabulate(data, headers=headers, tablefmt="grid"))

    def limit_displayed_rows(self, n):
        # Display the first n rows of the DataFrame
        print("Displaying the limited n rows of the DataFrame")
        self.employee_df.limit(n).show()

    def display_dataframe_vertically(self):
        # Display the DataFrame vertically
        print("Displaying the DataFrame vertically")
        self.employee_df.show(n=self.employee_df.count(), truncate=False, vertical=True)

    def display_dataframe_with_truncate(self):
        # Display the DataFrame with truncated columns
        print("Displaying the DataFrame with truncated columns")
        self.employee_df.show(n=self.employee_df.count(), truncate=True)

    def display_dataframe_with_all_parameters(self):
        # Display the DataFrame with all parameters
        print("Displaying the DataFrame with all parameters")
        self.employee_df.show(n=self.employee_df.count(), truncate=True, vertical=True)

    def display_dataframe_as_pandas(self):
        # Display the DataFrame as a Pandas DataFrame
        print("Displaying the DataFrame as a Pandas DataFrame")
        pandas_df = self.employee_df.toPandas()
        print(pandas_df.head())

    def adjust_column_width(self):
        # Adjust the column width
        print("Adjusting the column width")
        self.spark.conf.set("spark.sql.repl.eagerEval.maxNumOfFields", "100")
        self.employee_df.show()

    def truncate_cell_content(self):
        # Truncate the cell content of the employee_name column
        print("Truncating the cell content of the employee_name column")
        truncated_df = self.employee_df.withColumn("truncated_employee_name", substring("employee_name", 1, 4))
        truncated_df.show()

    def run_examples(self):
        self.display_employee_df()
        self.display_specific_columns("employee_name", "department")
        self.display_first_n_rows(5)
        self.display_last_n_rows(5)
        self.limit_displayed_rows(4)
        self.display_dataframe_vertically()
        self.display_dataframe_with_truncate()
        self.display_dataframe_with_all_parameters()
        self.display_dataframe_as_pandas()
        self.adjust_column_width()
        self.truncate_cell_content()

# Instantiate the class and run the examples
learn = DoWhileLearnShowDataFrame()
learn.run_examples()