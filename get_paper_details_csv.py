"""This file fetches Open Access research papers with valid URLs from the OpenAlex API and saves the details into a CSV file, this csv is used as a test dataset for selenum scraping."""

import csv
import pandas as pd
from pyalex import Works, config

config.email = "mahdee.m.kamal@gmail.com"


def fetch_oa_papers(n=100, filename="oa_papers.csv"):
    query = Works().filter(is_oa=True, has_pdf_url=True, has_doi=True)

    pager = query.paginate(per_page=100)

    results_collected = []

    print(f"Fetching {n} Open Access papers with valid URLs...")

    for page in pager:
        for work in page:
            if len(results_collected) >= n:
                break

            # Accessing the nested 'open_access' object safely
            oa_info = work.get("open_access", {})
            oa_url = oa_info.get("oa_url")

            if oa_url:
                results_collected.append(
                    {
                        "oa_url": oa_url,
                        "id": work.get("id"),
                        "title": work.get("title"),
                        "publication_year": work.get("publication_year"),
                        "is_oa": oa_info.get("is_oa"),
                        "doi": work.get("doi"),
                    }
                )

        if len(results_collected) >= n:
            break

    if not results_collected:
        print("No papers found matching those criteria.")
        return

    df = pd.DataFrame(results_collected)
    # df.sort_values(by="oa_url", inplace=True)
    df.to_csv(filename, index=False, encoding="utf-8")

    print(f"Successfully saved {len(results_collected)} records to {filename}")


if __name__ == "__main__":
    fetch_oa_papers(n=200)
