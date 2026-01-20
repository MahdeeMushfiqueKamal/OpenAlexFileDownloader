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

    # Custom download loop with progress tracking
    results = {"successful": 0, "failed": 0, "total": len(urls_to_download)}
    url_to_filename = {}

    print(f"\n{'='*60}")
    print(f"Starting batch download of {results['total']} URLs")
    print(f"{'='*60}\n")

    for idx, url in enumerate(urls_to_download, 1):
        print(f"\n{'='*60}")
        print(f"Processing URL {idx}/{results['total']}")
        print(f"{'='*60}")

        files_before = downloader.downloaded_files.copy()

        try:
            success = downloader.default_pdf_downloader(url)
        except Exception as e:
            print(f"Exception during download: {str(e)}")
            success = False

        if success:
            results["successful"] += 1

            new_files = downloader.downloaded_files - files_before
            if new_files:
                new_filename = list(new_files)[0]
                url_to_filename[url] = new_filename
                print(f"✓ Mapped: {url} -> {new_filename}")
            else:
                url_to_filename[url] = ""
                print(f"⚠ Download successful but no new file detected")
        else:
            results["failed"] += 1
            url_to_filename[url] = ""
            print(f"✗ Download failed")

        success_rate = (results["successful"] / idx * 100) if idx > 0 else 0
        print(f"\n{'─'*60}")
        print(f"PROGRESS UPDATE [{idx}/{results['total']}]:")
        print(f"  ✓ Successful: {results['successful']}")
        print(f"  ✗ Failed: {results['failed']}")
        print(f"  Success Rate: {success_rate:.1f}%")
        print(f"{'─'*60}")

        # Wait before next download (except for last one)
        if idx < results["total"]:
            import random
            import time

            delay = random.uniform(downloader.MIN_PAGE_DELAY, downloader.MAX_PAGE_DELAY)
            print(f"\nWaiting {delay:.2f}s before next download...\n")
            time.sleep(delay)

    # Final summary
    print(f"\n{'='*60}")
    print("FINAL RESULTS:")
    print(f"  Total URLs: {results['total']}")
    print(f"  Successfully Downloaded: {results['successful']} PDFs")
    print(f"  Failed: {results['failed']} PDFs")
    print(f"  Success Rate: {results['successful']/results['total']*100:.1f}%")
    print(f"{'='*60}")

    # Add filename column to the original dataframe
    df["downloaded_filename"] = df["oa_url"].map(url_to_filename).fillna("")
    output_csv = "oa_papers_with_filenames.csv"
    df.to_csv(output_csv, index=False)

    print(f"\nUpdated CSV saved to: {output_csv}")
    print(f"Column added: 'downloaded_filename'")

    unsuccessful_urls = [
        url for url, filename in url_to_filename.items() if filename == ""
    ]
    unsuccessful_urls.sort()

    if unsuccessful_urls:
        with open("unsuccessful_urls.txt", "w") as f:
            for url in unsuccessful_urls:
                f.write(url + "\n")
        print(f"Unsuccessful URLs saved to: unsuccessful_urls.txt")
    else:
        print(f"All downloads successful! No unsuccessful URLs.")

    downloader.cleanup()
    print("\nDownload process completed!")


if __name__ == "__main__":
    main()
