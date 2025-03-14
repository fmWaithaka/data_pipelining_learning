import pandas as pd
import os
import logging

logging.basicConfig(
    filename="dataPipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)


def join_data(orders_path="/tmp/orders/data-00000",
              customers_path="/tmp/customers/data-00000",
              output_path="/tmp/join_orders_and_customers/data-00000"):
    """
    Reads orders and customers data, performs an inner join,
    aggregates order count per customer, and saves the result.

    Args:
        orders_path (str): Path to the orders CSV file.
        customers_path (str): Path to the customers CSV file.
        output_path (str): Path to save the output CSV file.

    Returns:
        None
    """
    try:
        logging.info("Reading orders and customers data...")

        if not os.path.exists(orders_path):
            logging.error(f"Orders file not found: {orders_path}")
            return
        if not os.path.exists(customers_path):
            logging.error(f"Customers file not found: {customers_path}")
            return

        # Read data
        orders = pd.read_csv(orders_path)
        customers = pd.read_csv(customers_path)

        # Validate required columns
        if 'order_customer_id' not in orders.columns or 'customer_id' not in customers.columns:
            logging.error("Missing required columns in input files.")
            return

        logging.info("Performing inner join on 'customer_id' and 'order_customer_id'...")
        merged_df = customers.merge(orders, left_on="customer_id", right_on="order_customer_id", how="inner")

        # Aggregate order counts per customer
        logging.info("Calculating order count per customer...")
        order_count_by_customer = merged_df.groupby('customer_id').size().reset_index(name='order_count')

        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Save the result
        order_count_by_customer.to_csv(output_path, index=False)
        logging.info(f"Successfully saved joined data to {output_path}")

    except Exception as e:
        logging.error(f"Error in join_data: {e}", exc_info=True)


if __name__ == '__main__':
    join_data()
