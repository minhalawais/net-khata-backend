import uuid
from werkzeug.security import generate_password_hash
import sys

def generate_queries(company_name, username, password, email):
    company_id = str(uuid.uuid4())
    user_id = str(uuid.uuid4())
    password_hash = generate_password_hash(password)
    
    company_query = f"""
INSERT INTO companies (id, name, address, contact_number, email, is_active)
VALUES (
    '{company_id}', 
    '{company_name}', 
    'Update with actual address', 
    '000-0000000', 
    '{email}', 
    true
);"""

    user_query = f"""
INSERT INTO users (id, company_id, username, password, email, role, is_active)
VALUES (
    '{user_id}', 
    '{company_id}', 
    '{username}', 
    '{password_hash}', 
    '{email}', 
    'company_owner', 
    true
);"""

    return company_query, user_query

if __name__ == "__main__":
    print("-" * 30)
    print("New Company Profile & Credentials Generator")
    print("-" * 30)
    
    company_name = input("Enter Company Name: ")
    username = input("Enter Username: ")
    password = input("Enter Password: ")
    email = input("Enter Admin Email: ")
    
    c_query, u_query = generate_queries(company_name, username, password, email)
    
    print("\n" + "=" * 50)
    print("PostgreSQL Queries")
    print("=" * 50)
    print(c_query)
    print(u_query)
    print("=" * 50)
    print("\nSave these queries and run them in your database.")
