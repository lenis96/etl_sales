import pandas as pd
import boto3
from io import StringIO
import psycopg2
from constants import BUCKET_NAME,IAM_ROLE, REDSHIFT_USER, REDSHIFT_DB, REDSHIFT_PASSWORD, REDSHIFT_ENDPOINT, REDSHIFT_PORT


def generate_date_dimension(start_date, end_date):
    """
    Generate a date dimension DataFrame from start_date to end_date.
    
    Parameters:
        start_date (str): Start date in YYYY-MM-DD format.
        end_date (str): End date in YYYY-MM-DD format.
    
    Returns:
        DataFrame: Date dimension DataFrame with columns 'date', 'year', 'month', 'day', 'weekday'.
    """
    # Generate date range
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    # Create DataFrame
    df = pd.DataFrame(date_range, columns=['date'])
    
    # Extract year, month, day, and weekday
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['day'] = df['date'].dt.day
    df['weekday'] = df['date'].dt.day_of_week
    df['date_id'] = df.index + 1
    df['date'] = df['date'].astype(str)
    
    return df

class ETLSales():

    def upload_redshift(self,bucket_name:str,redshift_db:str,redshift_user:str,redshift_password:str,redshift_endpoint:str,redshift_port:int):
        conn = psycopg2.connect(
            dbname=redshift_db,
            user=redshift_user,
            password=redshift_password,
            host=redshift_endpoint,
            port=redshift_port
        )

        create_table_query = """
            CREATE TABLE IF NOT EXISTS branches (
                branch_id INTEGER PRIMARY KEY,
                branch VARCHAR(100),
                city VARCHAR(100)
            );

            CREATE TABLE IF NOT EXISTS dates (
                customer_id INTEGER PRIMARY KEY,
                customer_type VARCHAR(100)
            );

            CREATE TABLE IF NOT EXISTS genders (
                gender_id INTEGER PRIMARY KEY,
                gender VARCHAR(100)
            );

            CREATE TABLE IF NOT EXISTS product_lines (
                product_line_id INTEGER PRIMARY KEY,
                product_line VARCHAR(100)
            );

            CREATE TABLE IF NOT EXISTS payment (
                payment_id INTEGER PRIMARY KEY,
                payment VARCHAR(100)
            );

            CREATE TABLE IF NOT EXISTS dates (
                date_id INTEGER PRIMARY KEY,
                c_date DATE,
                c_year INTEGER,
                c_month SMALLINT,
                c_day SMALLINT,
                c_weekday SMALLINT
            );

            CREATE TABLE IF NOT EXISTS dates (
                id INTEGER PRIMARY KEY,
                invoice_id VARCHAR(20),
                unit_price DECIMAL(7,2),
                quantity SMALLINT,
                tax_5 REAL,
                Total REAL,
                cogs REAL,
                gross_margin_percentage REAL,
                gross_income REAL,
                rating DECIMAL(2,1),
                branch_id INTEGER,
                customer_id INTEGER,
                gender_id INTEGER,
                product_line_id INTEGER,
                payment_id INTEGER,
                date_id INTEGER,
                FOREIGN KEY (branch_id) REFERENCES branches(branch_id),
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
                FOREIGN KEY (gender_id) REFERENCES genders(gender_id),
                FOREIGN KEY (product_line_id) REFERENCES product_lines(product_line_id),
                FOREIGN KEY (payment_id) REFERENCES payments(payment_id),
                FOREIGN KEY (date_id) REFERENCES dates(date_id)
            );
            """
        with conn.cursor() as cursor:
            cursor.execute(create_table_query)
            conn.commit()
        
        copy_query = f"""
            COPY branches
            FROM 's3://{bucket_name}/data/branch.csv'
            IAM_ROLE '{IAM_ROLE}'
            CSV
            IGNOREHEADER 1;
            
            COPY customer_types
            FROM 's3://{bucket_name}/data/customer_type.csv'
            IAM_ROLE '{IAM_ROLE}'
            CSV
            IGNOREHEADER 1;

            COPY genders
            FROM 's3://{bucket_name}/data/gender.csv'
            IAM_ROLE '{IAM_ROLE}'
            CSV
            IGNOREHEADER 1;

            COPY product_lines
            FROM 's3://{bucket_name}/data/product_line.csv'
            IAM_ROLE '{IAM_ROLE}'
            CSV
            IGNOREHEADER 1;

            COPY payments
            FROM 's3://{bucket_name}/data/payment.csv'
            IAM_ROLE '{IAM_ROLE}'
            CSV
            IGNOREHEADER 1;

            COPY dates
            FROM 's3://{bucket_name}/data/dates.csv'
            IAM_ROLE '{IAM_ROLE}'
            CSV
            IGNOREHEADER 1;
        """

        
        with conn.cursor() as cursor:
            cursor.execute(copy_query)
            conn.commit()
            print("Data loaded into Redshift")

        conn.close()

    def get_csv(self):
        s3_client = boto3.client('s3')
        bucket_name = 'lenis-test2'
        file_key = 'sales.csv'
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        csv_content = response['Body'].read().decode('utf-8')
        csv_string_io = StringIO(csv_content)
        df = pd.read_csv(csv_string_io)
        return df

    def get_dim(self,df: pd.DataFrame,columns: list,id_column_name: str):
        df_dim = df[columns].drop_duplicates()
        # df_dim
        df_dim = df_dim.reset_index()
        df_dim.drop(columns='index',inplace=True)
        df_dim[id_column_name] = df_dim.index + 1
        # df_dim
        return df_dim

    def transform(self,df: pd.DataFrame):

        df_branch = self.get_dim(df,['City','Branch'],'branch_id')
        
        df_customer_type = self.get_dim(df,['Customer type'],'customer_id')
        

        df_gender = self.get_dim(df,['Gender'],'gender_id')
        

        df_product_line = self.get_dim(df,['Product line'],'product_line_id')
        

        df_payment = self.get_dim(df,['Payment'],'payment_id')
        

        df['Date'] = pd.to_datetime(df['Date'])

        min_date = df['Date'].min().strftime('%Y-%m-%d')
        max_date = df['Date'].max().strftime('%Y-%m-%d')
        df['Date'] = df['Date'].astype(str)

        df_dates = generate_date_dimension(min_date,max_date)
        


        df_final = pd.merge(df,df_branch[['branch_id','Branch']],on='Branch')\
        .merge(df_customer_type,on='Customer type')\
        .merge(df_gender,on='Gender')\
        .merge(df_product_line,on='Product line')\
        .merge(df_payment,on='Payment')\
        .merge(df_dates[['date_id','date']],left_on='Date',right_on='date')
        df_final = df_final.drop(['Branch','City','Customer type','Gender','Product line','Payment','Payment','date','Date','Time'],axis=1)
        df_final['id'] = df_final.index + 1

        df_branch.rename(columns={'City':'city','Branch':'branch'},inplace=True)
        self.save_dim_to_csv(df_branch,'data/branch.csv')

        df_customer_type.rename(columns={'Customer type':'customer_type'},inplace=True)
        self.save_dim_to_csv(df_customer_type,'data/customer_type.csv')

        df_gender.rename(columns={'Gender':'gender'},inplace=True)
        self.save_dim_to_csv(df_gender,'data/gender.csv')

        df_product_line.rename(columns={'Product line':'product_line'},inplace=True)
        self.save_dim_to_csv(df_product_line,'data/product_line.csv')

        df_payment.rename(columns={'Payment':'payment'},inplace=True)
        self.save_dim_to_csv(df_payment,'data/payment.csv')

        self.save_dim_to_csv(df_dates,'data/dates.csv')

        df_final.rename(columns={'Invoice ID':'invoice_id','Unit price':'unit_price','Quantity':'quantity','Tax 5%':'tax_5','gross margin percentage':'gross_margin_percentage','gross income':'gross_income','Rating':'rating','Total':'total'},inplace=True)
        df_final.to_csv('data/df_final.csv',index=False)
        
    def save_dim_to_csv(self,df_dim: pd.DataFrame,path: str):
        df_dim.to_csv(path,index=False)

    def upload_csv(self):
        s3_client = boto3.client('s3')
        bucket_name = BUCKET_NAME
        s3_client.upload_file('data/branch.csv',bucket_name,'data/branch.csv')
        s3_client.upload_file('data/customer_type.csv',bucket_name,'data/customer_type.csv')
        s3_client.upload_file('data/gender.csv',bucket_name,'data/gender.csv')
        s3_client.upload_file('data/product_line.csv',bucket_name,'data/product_line.csv')
        s3_client.upload_file('data/payment.csv',bucket_name,'data/payment.csv')
        s3_client.upload_file('data/dates.csv',bucket_name,'data/dates.csv')
        s3_client.upload_file('data/df_final.csv',bucket_name,'data/df_final.csv')


if __name__ == "__main__":
    etl_sales = ETLSales()
    df = etl_sales.get_csv()
    etl_sales.transform(df)
    etl_sales.upload_csv()
    etl_sales.upload_redshift(BUCKET_NAME,REDSHIFT_DB,REDSHIFT_USER,REDSHIFT_PASSWORD,REDSHIFT_ENDPOINT,REDSHIFT_PORT)

