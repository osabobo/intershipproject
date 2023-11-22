import time
import json
import random
import html
import re
import traceback
import logging
import requests
from datetime import datetime
from word2number import w2n
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from selenium.common.exceptions import NoSuchElementException



logging.basicConfig(level=logging.INFO)


def process_review_count(text):
    text = text.strip().replace(',', '')
    if 'K+' in text:
        return str(int(float(text.replace('K+', '').strip()) * 1000))
    return text


def setup_driver():
    options = webdriver.EdgeOptions()
    options.add_argument('--no-sandbox')
    try:
        driver = webdriver.Edge(service=Service(EdgeChromiumDriverManager().install()), options=options)
    except Exception as e:
        print(e)
        raise Exception("Failed to install Edge Chromium driver.")
    return driver

def scrape_extra_parameters(url: str, driver: webdriver.Edge) -> dict:
    try:
        driver.get(url)
        try:
            WebDriverWait(driver, 20).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div[data-hook='review']")))

        except TimeoutException:
            print(f"TimeoutException: Could not find reviews for {url}")
            return {}
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # Extract the general reviews
        reviews_tags = soup.find_all('div', attrs={'data-hook': 'review'})

        result = {}
        for i, review_tag in enumerate(reviews_tags[:5]):
            result[f'Customer_{i + 1}_ID'] = review_tag.attrs.get('id', 'None')
            
            # Extract the Star Rating
            star_rating_tag = review_tag.select_one('i[data-hook="review-star-rating"] span.a-icon-alt')
            star_rating = float(star_rating_tag.text.split()[0]) if star_rating_tag else 0.0
            result[f'Customer_{i+1}_Star_Rating'] = star_rating

            # Extract the Comment Title
            comment_title_tag = review_tag.select_one('a[data-hook="review-title"]')
            # Inside the for loop, after extracting the comment title:
            if comment_title_tag:
                actual_comment_title = comment_title_tag.text.strip()
            else:
                # Handle alternate structure
                comment_title_tag = review_tag.select_one('span.cr-original-review-content')
                actual_comment_title = comment_title_tag.text.strip() if comment_title_tag else 'NaN'

            # Remove the pattern "k out of 5 stars\n" from the comment
            actual_comment_title = re.sub(r'\d+(\.\d+)? out of 5 stars\n', '', actual_comment_title)

            result[f'Customer_{i+1}_Comment'] = actual_comment_title

            # Extract the Number of people who found the review helpful
            helpful_vote_tag = review_tag.select_one('span[data-hook="helpful-vote-statement"]')
            helpful_count = w2n.word_to_num(helpful_vote_tag.text.split()[0]) if helpful_vote_tag else 0
            result[f'Customer_{i+1}_buying_influence'] = helpful_count

            # Extract the post time
            customer_id = result[f'Customer_{i + 1}_ID']  # Extract the customer ID from the results
            Cust_tags_date = review_tag.select(f'#customer_review-{customer_id} > span')  # Use the customer ID in the selector

            if Cust_tags_date:
                Cust_post_time_text = Cust_tags_date[0].text.strip()
                match = re.search(r'on (.+)$', Cust_post_time_text)
                if match:
                    date_string = match.group(1)
                    try:
                        post_date = datetime.strptime(date_string, '%B %d, %Y')
                        result[f'Customer_{i+1}_Date'] = post_date.isoformat()                            
                    except ValueError as ve:
                        print(f"Error parsing date string {date_string}: {ve}")
                        result[f'Customer_{i+1}_Date'] = '-'
                else:
                    print("Date not found in text:", Cust_post_time_text)
                    result[f'Customer_{i+1}_Date'] = '-'
            else:
                print("Date tag not found")
                result[f'Customer_{i+1}_Date'] = None
                 

        # Extract Top Positive and Critical Reviews (Moved outside of the above loop)
        Parent_review_tags = soup.select('div[id^="viewpoint-"]')
        if len(Parent_review_tags) > 0: 
            ts = 'positive-review'
            result.update(extract_specific_review(Parent_review_tags[0], 'Top_Positive', ts, soup, url))

        else:
            result.update(set_default_values('Top_Positive'))
            
        if len(Parent_review_tags) > 1: 
            ts = 'critical-review.a-span-last'
            result.update(extract_specific_review(Parent_review_tags[1], 'Critical', ts, soup, url))

        else:
            result.update(set_default_values('Critical'))
            
        return result
    except Exception as e:
        print(f"Error scraping extra parameters: {e}")
        traceback.print_exc()
        return {}

def extract_specific_review(review_tag, review_type, ts, soup, url):
    specific_result = {}
    
    # Extracting ID
    review_id = review_tag.get('id', 'None').replace('viewpoint-', '')
    specific_result[f'{review_type}_Review_Cust_ID'] = review_id

    # # Extract Customer Name and Influenced
 
     # Corrected Extraction for Customer Name
    customer_name_selector = 'div.a-profile-content span.a-profile-name'
    specific_result[f'{review_type}_Review_Cust_Name'] = review_tag.select_one(customer_name_selector).text if review_tag.select_one(customer_name_selector) else 'None'

    # Corrected Selector
    influenced_selector = f'div.a-column.a-span6.view-point-review.{ts} div.a-row.a-spacing-top-small span.a-size-small.a-color-tertiary span.review-votes'
    influenced_element = soup.select_one(influenced_selector)

    if influenced_element:
        # Directly extract the text from the found element
        helpful_text = influenced_element.text.strip()
        print("Helpful Text:", helpful_text)  # Debugging line
        
        # Check if the text starts with a digit and extract the first contiguous digit sequence
        match = re.match(r'\d+', helpful_text)
        if match:
            helpful_count = int(match.group())
        else:
            # If the text doesn't start with a digit, try converting the first word to a number
            helpful_count = w2n.word_to_num(helpful_text.split()[0])
    else:
        print(f"Tag not found in {url}")  # Debugging line
        helpful_count = 0

    specific_result[f'{review_type}_Review_Cust_Influenced'] = helpful_count

    
    # Extract Customer Review Comment
    review_comment_tag = review_tag.find('div', class_='a-row a-spacing-top-mini')
    specific_result[f'{review_type}_Review_Cust_Comment'] = review_comment_tag.text.strip() if review_comment_tag else 'None'
    
    # Extract Customer Review Title
    review_title_tag = review_tag.select_one('span[data-hook="review-title"]')
    specific_result[f'{review_type}_Review_Cust_Comment_Title'] = review_title_tag.text if review_title_tag else 'None'
    
    # Extract the post time
    review_tags_date = review_tag.select('div.a-expander-content.a-expander-partial-collapse-content span.a-size-base.a-color-secondary.review-date')
    if review_tags_date:
        post_time_text = review_tags_date[0].text.strip()
        match = re.search(r'on (.+)$', post_time_text)
        if match:
            date_string = match.group(1)
            try:
                post_date = datetime.strptime(date_string, '%B %d, %Y')
                specific_result[f'{review_type}_Review_Cust_Date'] = post_date.isoformat()                            
            except ValueError as ve:
                print(f"Error parsing date string {date_string}: {ve}")
                specific_result[f'{review_type}_Review_Cust_Date'] = '-'
        else:
            print("Date not found in text:", post_time_text)
            specific_result[f'{review_type}_Review_Cust_Date'] = '-'
    else:
        print("Date tag not found")
        specific_result[f'{review_type}_Review_Cust_Date'] = None
    
    
    # Extract the Star Rating
    review_star_rating_tag = review_tag.select_one('i[data-hook="review-star-rating-view-point"] span.a-icon-alt')
    star_rating = float(review_star_rating_tag.text.split()[0]) if review_star_rating_tag else 0.0
    specific_result[f'{review_type}_Review_Cust_Star_Rating'] = star_rating
    
    return specific_result

def set_default_values(review_type):
    default_values = {
        f'{review_type}_Review_Cust_ID': 'None',
        f'{review_type}_Review_Cust_Name': 'None',
        f'{review_type}_Review_Cust_Comment': 'None',
        f'{review_type}_Review_Cust_Comment_Title': 'None',
        f'{review_type}_Review_Cust_Influenced': 0,
        f'{review_type}_Review_Cust_Star_Rating': 0.0,
        f'{review_type}_Review_Cust_Date': None,
    }
    return default_values

def get_title(soup):
    try:
        title = soup.find("span", attrs={"id": 'productTitle'})
        title_string = title.string.strip()
    except AttributeError:
        title_string = ""
    return title_string

def get_price(soup):
    try:
        # Try the first potential structure
        price_element = soup.find("span", class_="aok-offscreen")
        if not price_element:  
            # Try the second potential structure if the first one doesn't exist
            price_element = soup.find("span", class_="a-offscreen")
            
        # Extract the price from the text using regular expressions
        price_match = re.search(r"\$([\d,]+\.\d{2})", price_element.text)
        if price_match:
            price = price_match.group(1)
        else:
            price = ""
    except AttributeError:
        price = ""
    return price


def get_rating(soup):
    try:
        # Extract rating based on the provided structure
        rating_element = soup.select("#acrPopover > span.a-declarative > a > span")
        if rating_element:
            rating = rating_element[0].text.strip()
        else:
            rating = ""
    except AttributeError:
        rating = ""
    return rating



def get_review_count(soup):
    try:
        review_count_element = soup.select_one("#acrCustomerReviewText")
        reviews_text = review_count_element.text.strip()
        review_count = process_review_count(reviews_text)
    except (AttributeError, ValueError):
        review_count = 0
    return review_count



def process_review_count(reviews_text):
    if "K+" in reviews_text:
        # Remove 'K+', convert to float and multiply by 1000
        return int(float(reviews_text.replace("K+", "").strip()) * 1000)
    else:
        # Remove commas, then remove the word 'ratings', and finally convert to integer
        clean_text = reviews_text.replace(",", "").replace("ratings", "").strip()
        return int(clean_text)



def extract_product_details(driver, asin):
    product_page_url = f"https://www.amazon.com/dp/{asin}"
    reviews_page_url = f"https://www.amazon.com/product-reviews/{asin}/ref=cm_cr_dp_d_show_all_top?ie=UTF8&reviewerType=all_reviews&sortBy=recent"
    
    # Navigate to the product page for scraping
 
    driver.get(product_page_url)
    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, 'productTitle')))
    
    # Create a BeautifulSoup object and parse the page source
    soup = BeautifulSoup(driver.page_source, 'lxml')

    # Extract details using the dedicated functions and store in a dictionary
    product_dict = {
        'Product_ID': asin,
        'product': get_title(soup),
        'price': get_price(soup),
        'ratings': get_rating(soup),
        'reviews': get_review_count(soup),
        'url': reviews_page_url  # Storing the reviews page URL for future use
    }

    return product_dict

# Define a list of user agents to mimic different browsers and devices
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/17.17134"
]

def setup_driver_with_random_user_agent():
    options = webdriver.EdgeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument(f"user-agent={random.choice(user_agents)}")  # Set a random user agent
    try:
        driver = webdriver.Edge(service=Service(EdgeChromiumDriverManager().install()), options=options)
    except Exception as e:
        print(e)
        raise Exception("Failed to install Edge Chromium driver.")
    return driver

def emulate_human_scrolling(driver, scroll_pause_time=2):
    # Get scroll height initially
    last_height = driver.execute_script("return document.body.scrollHeight")

    while True:
        # Scroll down to the bottom of the page
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

        # Wait for page to load
        time.sleep(scroll_pause_time)

        # Calculate new scroll height and compare with the last scroll height
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

def extract_data_asins_from_html(html_content):
    # Using BeautifulSoup to parse the HTML content
    soup = BeautifulSoup(html_content, 'lxml')
    
    # Extract all elements with 'data-asin' attribute
    elements_with_data_asin = soup.find_all(attrs={"data-asin": True})
    
    # Gather elements with the data-asin attribute that have non-empty values
    distinct_product_asins = set([element['data-asin'] for element in elements_with_data_asin if element['data-asin'].strip()])
    
    return distinct_product_asins

def scrape_amazon(categories):
    driver = setup_driver_with_random_user_agent()  # Setup driver with a random user agent
    all_products = []
    seen_products = set()

    for category, base_url in categories.items():
        for page in range(1, 20):
            url = f"{base_url}&page={page}"

            try:
                driver.get(url)
                emulate_human_scrolling(driver, scroll_pause_time=random.randint(2, 4))
                WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#search > div.s-desktop-width-max.s-desktop-content.s-wide-grid-style-t1.s-opposite-dir.s-wide-grid-style.sg-row > div.sg-col-20-of-24.s-matching-dir.sg-col-16-of-20.sg-col.sg-col-8-of-12.sg-col-12-of-16 > div > span.rush-component.s-latency-cf-section > div.s-main-slot.s-result-list.s-search-results.sg-row")))
            except TimeoutException:
                print(f"Timed out waiting for elements on page {page} of category {category}.")
                continue

            time.sleep(random.uniform(3.0, 6.0))
            distinct_asins = extract_data_asins_from_html(driver.page_source)
            print(f"Extracted ASINs: {distinct_asins}")

            for asin in distinct_asins:
                try:
                    print(f"Processing ASIN: {asin}")
                    product_details = extract_product_details(driver, asin)
                    product_details['Product_ID'] = asin
                    product_details['category'] = category

                    identifier = product_details['Product_ID']
                    if identifier and identifier not in seen_products:
                        seen_products.add(identifier)
                        # You need to define and implement the scrape_extra_parameters function
                        extra_params = scrape_extra_parameters(product_details['url'], driver)
                        product_details.update(extra_params)
                        all_products.append(product_details)

                except Exception as e:
                    print(f"Error processing ASIN {asin}: {e}")

    driver.quit()
    return json.dumps(all_products)

if __name__ == '__main__':
    categories = {
        'Smartphones': 'https://www.amazon.com/s?k=smartphone&ref=nb_sb_noss',
        'Laptops': 'https://www.amazon.com/s?k=Laptops&ref=nb_sb_noss',
        'video_games': 'https://www.amazon.com/s?k=video_games&ref=nb_sb_noss',
        'Dresses':'https://www.amazon.com/s?k=Dresses&ref=nb_sb_noss',
        'Shoes':'https://www.amazon.com/s?k=Shoes&ref=nb_sb_noss',
        'Accessories':'https://www.amazon.com/s?k=accessories+for+clothes&ref=nb_sb_noss',
    }

    all_products = []
    try:
        all_products = json.loads(scrape_amazon(categories))
    except Exception as e:
        print(f"Error occurred during scraping: {e}")
    finally:
        with open('amazon_data_ext.json', 'w') as file:
            json.dump(all_products, file)
