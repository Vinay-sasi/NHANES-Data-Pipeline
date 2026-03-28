import os
import requests
import pandas as pd
import tempfile
import re
import logging
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs

# ---------------- CONFIG ---------------- #
BASE_URL = "https://wwwn.cdc.gov/nchs/nhanes/default.aspx"
YEAR_URL = "https://wwwn.cdc.gov/nchs/nhanes/continuousnhanes/default.aspx?Cycle=2021-2023"

DOWNLOAD_FOLDER = "downloads_nhanes"
RAW_FOLDER = os.path.join(DOWNLOAD_FOLDER, "raw")
DECODED_FOLDER = os.path.join(DOWNLOAD_FOLDER, "decoded")
CODEBOOK_FOLDER = os.path.join(DOWNLOAD_FOLDER, "codebooks")

os.makedirs(RAW_FOLDER, exist_ok=True)
os.makedirs(DECODED_FOLDER, exist_ok=True)
os.makedirs(CODEBOOK_FOLDER, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


# ---------------- UTILS ---------------- #
def generate_safe_filename(name):
    name = re.sub(r"[^\w\s-]", "", name)
    name = re.sub(r"[\s-]+", "_", name)
    return name[:100]


# ---------------- SCRAPING ---------------- #
def get_category_links():
    response = requests.get(YEAR_URL)
    soup = BeautifulSoup(response.text, "html.parser")

    categories = []

    for link in soup.find_all("a", href=True):
        href = link["href"]

        if "Component=" in href:
            full_url = urljoin(BASE_URL, href)

            if YEAR_URL.split("Cycle=")[1] in full_url:
                component = parse_qs(urlparse(full_url).query).get("Component", ["Unknown"])[0]
                categories.append((component, full_url))

    # Remove duplicate category names
    seen = set()
    unique_categories = []

    for name, url in categories:
        if name not in seen:
            seen.add(name)
            unique_categories.append((name, url))

    return unique_categories


def get_datasets(category_url):
    response = requests.get(category_url)
    soup = BeautifulSoup(response.text, "html.parser")

    datasets = []

    for row in soup.find_all("tr"):
        xpt_url = None
        codebook_url = None
        description = None

        for link in row.find_all("a", href=True):
            href = link["href"]

            if href.endswith(".xpt"):
                xpt_url = urljoin(BASE_URL, href) if href.startswith("/") else href

            if "Doc" in link.text:
                codebook_url = urljoin(BASE_URL, href) if href.startswith("/") else href

        desc_td = row.find("td", class_="text-left")
        if desc_td:
            description = desc_td.get_text(strip=True)

        if xpt_url and codebook_url and description:
            datasets.append((description, xpt_url, codebook_url))

    return datasets


# ---------------- DOWNLOAD ---------------- #
def download_xpt(xpt_url):
    response = requests.get(xpt_url)
    temp = tempfile.NamedTemporaryFile(delete=False, suffix=".xpt")
    temp.write(response.content)
    temp.close()
    return temp.name


def convert_xpt_to_csv(xpt_path, output_name):
    df = pd.read_sas(xpt_path, format="xport")
    os.remove(xpt_path)

    path = os.path.join(RAW_FOLDER, f"{output_name}.csv")
    df.to_csv(path, index=False)
    return path


def download_codebook(codebook_url, name):
    response = requests.get(codebook_url)
    soup = BeautifulSoup(response.text, "html.parser")

    data = []

    for page in soup.find_all("div", class_="pagebreak"):
        title = page.find("h3", class_="vartitle")

        header, desc = "", ""
        if title:
            parts = title.text.split("-", 1)
            header = parts[0].strip()
            desc = parts[1].strip() if len(parts) > 1 else ""

        table = page.find("table", class_="values")

        if table:
            headers = [th.text.strip() for th in table.find_all("th")]
            for row in table.find_all("tr"):
                values = [td.text.strip() for td in row.find_all("td")]
                if values:
                    row_dict = dict(zip(headers, values))
                    row_dict["Header Name"] = header
                    row_dict["Description"] = desc
                    data.append(row_dict)

    df = pd.DataFrame(data)
    path = os.path.join(CODEBOOK_FOLDER, f"{name}_codebook.csv")
    df.to_csv(path, index=False)
    return path


# ---------------- DECODING ---------------- #
def substitute_values(value, header_name, codebook):
    if pd.isna(value):
        return value

    # 🔥 FIX: handle byte strings
    if isinstance(value, bytes):
        value = value.decode('utf-8')

    relevant_entries = codebook[codebook["Header Name"] == header_name]

    for _, row in relevant_entries.iterrows():
        code_value = str(row.get("Code or Value", "")).strip()
        description = str(row.get("Value Description", "")).strip()

        if not code_value or not description:
            continue

        # Skip "Range of Values"
        if description.lower() == "range of values":
            return value

        # Range handling
        match = re.match(r"(\d+)\s*(to|-)\s*(\d+)", code_value)
        if match:
            try:
                if int(match.group(1)) <= float(value) <= int(match.group(3)):
                    return description
            except:
                pass

        # Exact match
        try:
            if float(value) == float(code_value):
                return description
        except:
            if str(value).strip().lower() == code_value.lower():
                return description

    return value

def process_data(raw_path, codebook_path, name):
    df = pd.read_csv(raw_path)
    codebook = pd.read_csv(codebook_path)

    codebook.columns = codebook.columns.str.strip()
    codebook["Header Name"] = codebook["Header Name"].astype(str).str.strip()

    for col in df.columns:
        if col in codebook["Header Name"].values:
            df[col] = df[col].apply(lambda x: substitute_values(x, col, codebook))

    mapping = dict(zip(codebook["Header Name"], codebook["Description"].fillna("")))
    df.rename(columns=mapping, inplace=True)

    output = os.path.join(DECODED_FOLDER, f"{name}_decoded.csv")
    df.to_csv(output, index=False)

    return output


# ---------------- INTERACTIVE FLOW ---------------- #
def main():
    print("\nFetching categories...\n")
    categories = get_category_links()

    for i, (name, _) in enumerate(categories):
        print(f"{i + 1}. {name}")

    choice = int(input("\nSelect a category number: ")) - 1
    category_name, category_url = categories[choice]

    print(f"\nFetching datasets for {category_name}...\n")
    datasets = get_datasets(category_url)

    if not datasets:
        print("❌ No datasets found. Try another category.")
        return

    for i, (desc, _, _) in enumerate(datasets):
        print(f"{i + 1}. {desc}")

    d_choice = int(input("\nSelect sub-category dataset: ")) - 1
    desc, xpt_url, codebook_url = datasets[d_choice]

    safe_name = generate_safe_filename(f"{category_name}_{desc}")

    logging.info("Downloading and processing...")

    xpt_path = download_xpt(xpt_url)
    raw_csv = convert_xpt_to_csv(xpt_path, safe_name)
    codebook_csv = download_codebook(codebook_url, safe_name)
    decoded_csv = process_data(raw_csv, codebook_csv, safe_name)

    print(f"\n✅ Done! File saved at:\n{decoded_csv}")


if __name__ == "__main__":
    main()