# path: scripts/list_product_titles.py
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

URL = "https://cannabisrealmny.com/white-plains/menu/categories/concentrates"

def main():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)

    driver.get(URL)
    html = driver.page_source
    driver.quit()

    soup = BeautifulSoup(html, "html.parser")

    # find all product cards
    products = soup.find_all("a", {"data-testid": "product-card-menu-link-body"})
    print(f"Found {len(products)} products\n")

    for idx, product in enumerate(products, start=1):
        # title is inside the <div data-testid="product-name-...">
        title_div = product.find("div", {"data-testid": lambda v: v and v.startswith("product-name-")})
        if title_div:
            print(f"{idx}. {title_div.get_text(strip=True)}")
        else:
            print(f"{idx}. [NO TITLE FOUND]")

if __name__ == "__main__":
    main()
