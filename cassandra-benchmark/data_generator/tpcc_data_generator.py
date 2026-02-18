"""
TPC-C data generator for Cassandra benchmark.
Generates realistic test data based on TPC-C specifications.
"""

import random
import string
from typing import Dict, Any, List
from datetime import datetime, timedelta


class TPCCDataGenerator:
    """Generates TPC-C compliant test data."""
    
    def __init__(self, num_warehouses: int = 10, 
                 num_districts_per_warehouse: int = 10,
                 num_customers_per_district: int = 3000,
                 num_items: int = 100000):
        """
        Initialize data generator with scale factors.
        
        Args:
            num_warehouses: Number of warehouses
            num_districts_per_warehouse: Districts per warehouse
            num_customers_per_district: Customers per district
            num_items: Total number of items
        """
        self.num_warehouses = num_warehouses
        self.num_districts_per_warehouse = num_districts_per_warehouse
        self.num_customers_per_district = num_customers_per_district
        self.num_items = num_items
        
        # Common last names for TPC-C
        self.last_names = [
            'SMITH', 'JONES', 'WILLIAMS', 'BROWN', 'JOHNSON',
            'MILLER', 'DAVIS', 'GARCIA', 'RODRIGUEZ', 'WILSON'
        ]
    
    def generate_random_string(self, min_len: int, max_len: int) -> str:
        """Generate random alphanumeric string."""
        length = random.randint(min_len, max_len)
        return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))
    
    def generate_random_numeric_string(self, length: int) -> str:
        """Generate random numeric string."""
        return ''.join(random.choices(string.digits, k=length))
    
    def generate_zip(self) -> str:
        """Generate random ZIP code."""
        return self.generate_random_numeric_string(4) + '11111'
    
    def generate_warehouse(self, w_id: int) -> Dict[str, Any]:
        """
        Generate warehouse data.
        
        Args:
            w_id: Warehouse ID
            
        Returns:
            Warehouse data dictionary
        """
        return {
            'w_id': w_id,
            'w_name': self.generate_random_string(6, 10),
            'w_street_1': self.generate_random_string(10, 20),
            'w_street_2': self.generate_random_string(10, 20),
            'w_city': self.generate_random_string(10, 20),
            'w_state': self.generate_random_string(2, 2),
            'w_zip': self.generate_zip(),
            'w_tax': round(random.uniform(0.0, 0.2), 4),
            'w_ytd': 300000.00
        }
    
    def generate_district(self, d_id: int, w_id: int) -> Dict[str, Any]:
        """
        Generate district data.
        
        Args:
            d_id: District ID
            w_id: Warehouse ID
            
        Returns:
            District data dictionary
        """
        return {
            'd_id': d_id,
            'd_w_id': w_id,
            'd_name': self.generate_random_string(6, 10),
            'd_street_1': self.generate_random_string(10, 20),
            'd_street_2': self.generate_random_string(10, 20),
            'd_city': self.generate_random_string(10, 20),
            'd_state': self.generate_random_string(2, 2),
            'd_zip': self.generate_zip(),
            'd_tax': round(random.uniform(0.0, 0.2), 4),
            'd_ytd': 30000.00,
            'd_next_o_id': 3001
        }
    
    def generate_customer(self, c_id: int, d_id: int, w_id: int) -> Dict[str, Any]:
        """
        Generate customer data.
        
        Args:
            c_id: Customer ID
            d_id: District ID
            w_id: Warehouse ID
            
        Returns:
            Customer data dictionary
        """
        return {
            'c_id': c_id,
            'c_d_id': d_id,
            'c_w_id': w_id,
            'c_first': self.generate_random_string(8, 16),
            'c_middle': 'OE',
            'c_last': random.choice(self.last_names),
            'c_street_1': self.generate_random_string(10, 20),
            'c_street_2': self.generate_random_string(10, 20),
            'c_city': self.generate_random_string(10, 20),
            'c_state': self.generate_random_string(2, 2),
            'c_zip': self.generate_zip(),
            'c_phone': self.generate_random_numeric_string(16),
            'c_since': datetime.now(),
            'c_credit': 'GC' if random.random() > 0.1 else 'BC',
            'c_credit_lim': 50000.00,
            'c_discount': round(random.uniform(0.0, 0.5), 4),
            'c_balance': -10.00,
            'c_ytd_payment': 10.00,
            'c_payment_cnt': 1,
            'c_delivery_cnt': 0,
            'c_data': self.generate_random_string(300, 500)
        }
    
    def generate_item(self, i_id: int) -> Dict[str, Any]:
        """
        Generate item data.
        
        Args:
            i_id: Item ID
            
        Returns:
            Item data dictionary
        """
        return {
            'i_id': i_id,
            'i_im_id': random.randint(1, 10000),
            'i_name': self.generate_random_string(14, 24),
            'i_price': round(random.uniform(1.0, 100.0), 2),
            'i_data': self.generate_random_string(26, 50)
        }
    
    def generate_stock(self, i_id: int, w_id: int) -> Dict[str, Any]:
        """
        Generate stock data.
        
        Args:
            i_id: Item ID
            w_id: Warehouse ID
            
        Returns:
            Stock data dictionary
        """
        return {
            's_i_id': i_id,
            's_w_id': w_id,
            's_quantity': random.randint(10, 100),
            's_dist_01': self.generate_random_string(24, 24),
            's_dist_02': self.generate_random_string(24, 24),
            's_dist_03': self.generate_random_string(24, 24),
            's_dist_04': self.generate_random_string(24, 24),
            's_dist_05': self.generate_random_string(24, 24),
            's_dist_06': self.generate_random_string(24, 24),
            's_dist_07': self.generate_random_string(24, 24),
            's_dist_08': self.generate_random_string(24, 24),
            's_dist_09': self.generate_random_string(24, 24),
            's_dist_10': self.generate_random_string(24, 24),
            's_ytd': 0,
            's_order_cnt': 0,
            's_remote_cnt': 0,
            's_data': self.generate_random_string(26, 50)
        }
    
    def generate_order(self, o_id: int, d_id: int, w_id: int, 
                      c_id: int) -> Dict[str, Any]:
        """
        Generate order data.
        
        Args:
            o_id: Order ID
            d_id: District ID
            w_id: Warehouse ID
            c_id: Customer ID
            
        Returns:
            Order data dictionary
        """
        return {
            'o_id': o_id,
            'o_d_id': d_id,
            'o_w_id': w_id,
            'o_c_id': c_id,
            'o_entry_d': datetime.now() - timedelta(days=random.randint(0, 365)),
            'o_carrier_id': random.randint(1, 10) if random.random() > 0.3 else None,
            'o_ol_cnt': random.randint(5, 15),
            'o_all_local': 1
        }
    
    def generate_order_line(self, ol_number: int, ol_o_id: int, 
                           ol_d_id: int, ol_w_id: int) -> Dict[str, Any]:
        """
        Generate order line data.
        
        Args:
            ol_number: Order line number
            ol_o_id: Order ID
            ol_d_id: District ID
            ol_w_id: Warehouse ID
            
        Returns:
            Order line data dictionary
        """
        return {
            'ol_o_id': ol_o_id,
            'ol_d_id': ol_d_id,
            'ol_w_id': ol_w_id,
            'ol_number': ol_number,
            'ol_i_id': random.randint(1, self.num_items),
            'ol_supply_w_id': ol_w_id,
            'ol_delivery_d': datetime.now() if random.random() > 0.3 else None,
            'ol_quantity': random.randint(1, 10),
            'ol_amount': round(random.uniform(0.01, 9999.99), 2),
            'ol_dist_info': self.generate_random_string(24, 24)
        }
    
    def generate_history(self, c_id: int, c_d_id: int, c_w_id: int,
                        d_id: int, w_id: int) -> Dict[str, Any]:
        """
        Generate history record.
        
        Args:
            c_id: Customer ID
            c_d_id: Customer district ID
            c_w_id: Customer warehouse ID
            d_id: District ID
            w_id: Warehouse ID
            
        Returns:
            History data dictionary
        """
        h_date = datetime.now() - timedelta(days=random.randint(0, 365))
        return {
            'h_c_id': c_id,
            'h_c_d_id': c_d_id,
            'h_c_w_id': c_w_id,
            'h_d_id': d_id,
            'h_w_id': w_id,
            'h_date': h_date,
            'h_amount': round(random.uniform(10.0, 5000.0), 2),
            'h_data': self.generate_random_string(12, 24),
            'date_bucket': h_date.strftime('%Y-%m-%d')
        }
    
    def get_scale_info(self) -> Dict[str, int]:
        """
        Get information about the data scale.
        
        Returns:
            Dictionary with scale information
        """
        total_districts = self.num_warehouses * self.num_districts_per_warehouse
        total_customers = total_districts * self.num_customers_per_district
        # Approximate stock items (items per warehouse)
        total_stock = self.num_warehouses * self.num_items
        
        return {
            'warehouses': self.num_warehouses,
            'districts': total_districts,
            'customers': total_customers,
            'items': self.num_items,
            'stock_records': total_stock,
            'estimated_total_records': (
                self.num_warehouses + 
                total_districts + 
                total_customers * 2 +  # customer and customer_by_name
                self.num_items +
                total_stock
            )
        }
