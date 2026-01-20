import pandas as pd
from pathlib import Path
from openalex_file_downloader import OpenAlexFileDownloader

headless_mode = False
download_dir = "openalex_downloads"


def main():
    df = pd.read_csv("oa_papers.csv")

    urls_to_download = df["oa_url"].dropna().tolist()
    print(f"Found {len(urls_to_download)} URLs to download")
    print(f"Headless mode: {headless_mode}")

    downloader = OpenAlexFileDownloader(
        urls=urls_to_download,
        download_directory=download_dir,
        headless=headless_mode,
        random_delay=True,
    )
    download_results, url_to_filename = downloader.download_all()

    print(f"\n{'='*60}")
    print("FINAL RESULTS:")
    print(f"  Total URLs: {download_results['total']}")
    print(f"  Successfully Downloaded: {download_results['successful']} PDFs")
    print(f"  Failed: {download_results['failed']} PDFs")
    print(
        f"  Success Rate: {download_results['successful']/download_results['total']*100:.1f}%"
    )
    print(f"{'='*60}")

    # Add filename column to the original dataframe
    df["downloaded_filename"] = df["oa_url"].map(url_to_filename).fillna("")
    output_csv = "oa_papers_with_filenames.csv"
    df.to_csv(output_csv, index=False)

    print(f"\nUpdated CSV saved to: {output_csv}")
    print(f"Column added: 'downloaded_filename'")

    # return a list of unsuccessful URLs
    unsuccessful_urls = [
        url for url, filename in url_to_filename.items() if filename == ""
    ]
    unsuccessful_urls.sort()

    if unsuccessful_urls:
        with open("unsuccessful_urls.txt", "w") as f:
            for url in unsuccessful_urls:
                f.write(url + "\n")
        print(f"Unsuccessful URLs saved to: unsuccessful_urls.txt")

    downloader.cleanup()
    print("\nDownload process completed!")


if __name__ == "__main__":
    main()
