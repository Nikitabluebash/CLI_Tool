import pandas as pd
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
import argparse
import time
from typing import Optional, Tuple

# Initialize geolocator with proper user agent
geolocator = Nominatim(user_agent="client-validator-cli", timeout=3)


def is_valid_email(email: str) -> bool:
    return isinstance(email, str) and "@" in email and "." in email


def is_non_empty_string(value: Optional[str]) -> bool:
    return isinstance(value, str) and value.strip() != ""


def build_address(street, locality, postcode) -> str:
    parts = [street, locality, postcode]
    return ", ".join([p.strip() for p in parts if is_non_empty_string(p)])


def fetch_coordinates(address: str, max_retries: int = 3) -> Tuple[Optional[float], Optional[float]]:
    for attempt in range(max_retries):
        try:
            print(f"Attempting geocode: {address}")
            location = geolocator.geocode(address)
            if location:
                return location.latitude, location.longitude
        except (GeocoderTimedOut, GeocoderServiceError) as e:
            print(f"Retry {attempt + 1}/{max_retries} after error: {e}")
            time.sleep(1)
    print(f"Failed to geocode: {address}")
    return None, None


def validate_and_enrich(df: pd.DataFrame) -> pd.DataFrame:
    valid_rows = []

    for index, row in df.iterrows():
        email = str(row.get("Email", "")).strip()
        first_name = str(row.get("First Name", "")).strip()
        last_name = str(row.get("Last Name", "")).strip()

        if not (is_valid_email(email) and is_non_empty_string(first_name) and is_non_empty_string(last_name)):
            print(f"Row {index} skipped: invalid name/email")
            continue

        # Extract address parts
        res_address = build_address(
            row.get("Residential Address Street"),
            row.get("Residential Address Locality"),
            row.get("Residential Address Postcode"),
        )

        postal_address = build_address(
            row.get("Postal Address Street"),
            row.get("Postal Address Locality"),
            row.get("Postal Address Postcode"),
        )

        full_address = res_address if is_non_empty_string(res_address) else postal_address
        if not is_non_empty_string(full_address):
            print(f"Row {index} skipped: missing address")
            continue

        lat, lon = fetch_coordinates(full_address)
        if lat is None or lon is None:
            print(f"Row {index} skipped: geolocation failed")
            continue

        row["latitude"] = lat
        row["longitude"] = lon
        valid_rows.append(row)

        time.sleep(1)  # Respect Nominatim rate limit

    return pd.DataFrame(valid_rows)


def main(input_file: str, output_file: str):
    try:
        df = pd.read_csv("/home/nikita/Python-Assignment/input_file_clients.csv").head(50)  # Limit to 50 rows for testing
        print(f"Loaded {len(df)} rows from {input_file}")
    except Exception as e:
        print(f"Error loading input file: {e}")
        return

    enriched_df = validate_and_enrich(df)
    enriched_df.to_csv(output_file, index=False)
    print(f"Saved {len(enriched_df)} valid rows to {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate and enrich client CSV data")
    parser.add_argument("input_file", help="/home/nikita/Python-Assignment/input_file_clients.csv")
    parser.add_argument("output_file", help="/home/nikita/Python-Assignment/outputfile.csv")
    args = parser.parse_args()

    main(args.input_file, args.output_file)


