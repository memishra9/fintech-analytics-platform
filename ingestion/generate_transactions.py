from faker import Faker
from datetime import datetime,timedelta
import random
import csv

fake= Faker()

CURRENCIES = ["INR", "USD", "GBP", "EUR", "SGD"]
STATUSES = ["completed", "failed", "pending", "reversed"]
PAYMENT_TYPES = ["UPI", "NEFT", "IMPS", "CARD", "WALLET"]
MERCHANTS = ["Amazon", "Flipkart", "Swiggy", "Zomato", "Uber", "Netflix"]

def generate_transaction(customer_ids: list[int]) ->dict:
    return{
        "transaction_id": fake.uuid4(),
        "customer_id" : random.choice(customer_ids),
        "amount" : round(random.uniform(10, 100000),2),
        "currency": random.choice(CURRENCIES) ,
        "status" : random.choice(STATUSES),
        "payment_type" : random.choice(PAYMENT_TYPES),
        "merchant":  random.choice(MERCHANTS),
        "timestamp" : (datetime.now() - timedelta(days=random.randint(0, 90) ,
                                                  hours= random.randint(0, 23)
                                                 )).isoformat()
    }

def generate_customers(n:int) ->list[dict]:
    return[
            {
            "customer_id":  fake.uuid4(),
            "name":         fake.name(),
            "email":        fake.email(),
            "city":         fake.city(),
            "country":      fake.country_code(),
            "segment":      random.choice(["retail", "premium", "business"]),
            "created_at":   fake.date_time_this_year().isoformat()
        }
        for _ in range(n)
    ]

def generate_fx_rates() -> list[dict]:
    base_rates = {
        "INR": 0.012, "USD": 1.0,
        "GBP": 1.27,  "EUR": 1.08, "SGD": 0.74
    }
    return [
        {
            "currency":    currency,
            "rate_to_usd": round(rate * random.uniform(0.98, 1.02), 6),
            "date":        datetime.now().date().isoformat()
        }
        for currency, rate in base_rates.items()
    ]

def save_to_csv(data: list[dict], filepath: str) -> None:
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    print(f"Saved {len(data)} rows to {filepath}")

if __name__ == "__main__":
    import os
    os.makedirs("data/raw", exist_ok=True)

    # generate customers first — transactions reference them
    customers    = generate_customers(500)
    customer_ids = [c["customer_id"] for c in customers]
    transactions = [generate_transaction(customer_ids) for _ in range(10000)]
    fx_rates     = generate_fx_rates()

    save_to_csv(customers,    "data/raw/customers.csv")
    save_to_csv(transactions, "data/raw/transactions.csv")
    save_to_csv(fx_rates,     "data/raw/fx_rates.csv")

    print("\nDone. Raw data ready:")
    print(f"  Customers:    {len(customers)}")
    print(f"  Transactions: {len(transactions)}")
    print(f"  FX Rates:     {len(fx_rates)}")

