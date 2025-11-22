#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time
import os
import sys
import threading
from typing import List, Dict, Set
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

skip_category_flag = threading.Event()
quit_flag = threading.Event()


def load_existing_products(output_json: str) -> Dict[str, Dict]:
    """Load existing products from JSON file to avoid duplicates."""
    if os.path.exists(output_json):
        try:
            with open(output_json, "r", encoding="utf-8") as f:
                products = json.load(f)
                return {p.get("product_url") or p.get("id"): p for p in products if p.get("product_url") or p.get("id")}
        except (json.JSONDecodeError, Exception) as e:
            print(f"[WARN] Could not load existing products: {e}")
    return {}


def save_products(output_json: str, products: Dict[str, Dict]):
    """Save products to JSON file."""
    products_list = list(products.values())
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(products_list, f, ensure_ascii=False, indent=2)
    print(f"[INFO] Saved {len(products_list)} total products to {output_json}")


def discover_food_categories(page) -> List[str]:
    """Discover all food category URLs from unimarc.cl."""
    fallback_categories = [
        "https://www.unimarc.cl/category/carnes",
        "https://www.unimarc.cl/category/frutas-y-verduras",
        "https://www.unimarc.cl/category/lacteos-huevos-y-refrigerados",
        "https://www.unimarc.cl/category/quesos-y-fiambres",
        "https://www.unimarc.cl/category/panaderia-y-pasteleria",
        "https://www.unimarc.cl/category/congelados",
        "https://www.unimarc.cl/category/despensa",
        "https://www.unimarc.cl/category/desayuno-y-dulces",
        "https://www.unimarc.cl/category/bebidas-y-licores"
    ]
    
    print(f"[INFO] Using {len(fallback_categories)} food categories from fallback list")
    return fallback_categories


def scrape_category_products_playwright(page, category_url: str, category_name: str, existing_products: Dict[str, Dict], output_json: str, max_products_per_category: int = 300) -> Dict[str, Dict]:
    """Scrape products from a category using Playwright, limited to max_products_per_category."""
    global skip_category_flag
    
    print(f"\n[INFO] Scraping category: {category_name} ({category_url})")
    print(f"[INFO] Max products per category: {max_products_per_category}")
    print(f"[INFO] 💡 Tip: Type 's' + Enter to skip this category, 'q' + Enter to quit")
    new_products = {}
    page_num = 1
    max_pages = 10
    
    while page_num <= max_pages:
        if skip_category_flag.is_set():
            print(f"\n[INFO] Skipping category: {category_name}")
            skip_category_flag.clear()
            break
        
        if len(new_products) >= max_products_per_category:
            print(f"[INFO] Reached maximum of {max_products_per_category} products for this category")
            break
        
        try:
            if page_num == 1:
                url = category_url
            else:
                url = f"{category_url}?page={page_num}"
            
            print(f"  [INFO] Loading page {page_num}...")
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(5000)
            
            products_data = page.evaluate(
                """
                () => {
                    const nextDataEl = document.getElementById('__NEXT_DATA__');
                    if (!nextDataEl) return [];
                    
                    const nextData = JSON.parse(nextDataEl.textContent);
                    const pageProps = nextData?.props?.pageProps;
                    if (!pageProps || !pageProps.dehydratedState) return [];
                    
                    const queries = pageProps.dehydratedState.queries || [];
                    if (queries.length === 0) return [];
                    
                    const queryData = queries[0].state.data;
                    const available = queryData.availableProducts || [];
                    const notAvailable = queryData.notAvailableProducts || [];
                    
                    return [...available, ...notAvailable];
                }
                """
            )
            
            valid_products = []
            for product in products_data:
                if len(new_products) >= max_products_per_category:
                    break
                
                product_id = product.get("productId") or product.get("itemId")
                product_name = product.get("name") or product.get("nameComplete")
                brand = product.get("brand")
                description = product.get("description")
                
                sellers = product.get("sellers", [])
                if sellers:
                    first_seller = sellers[0]
                    price = first_seller.get("price")
                    price_original = first_seller.get("listPrice")
                    in_stock = first_seller.get("availableQuantity", 0) > 0
                    unit_price = first_seller.get("ppum")
                else:
                    price = None
                    price_original = None
                    in_stock = False
                    unit_price = None
                
                images = product.get("images", [])
                image_url = images[0] if images else None
                
                sku = product.get("sku") or product.get("itemId")
                detail_url = product.get("detailUrl", "")
                product_url = f"https://www.unimarc.cl{detail_url}" if detail_url else None
                
                size = product.get("netContentLevelSmall") or product.get("netContent")
                
                promo_text = None
                price_detail = product.get("priceDetail", {})
                if price_detail:
                    promo_tag = price_detail.get("promotionalTag", {})
                    if promo_tag:
                        promo_text = promo_tag.get("text")
                
                key = product_url or product_id
                if key and key not in existing_products and key not in new_products:
                    product_dict = {
                        "id": product_id,
                        "name": product_name,
                        "brand": brand,
                        "category": category_name,
                        "price": int(price) if price else None,
                        "price_original": int(price_original) if price_original and price_original != price else None,
                        "currency": "CLP",
                        "size": size,
                        "unit_price": unit_price,
                        "image_url": image_url,
                        "product_url": product_url,
                        "in_stock": in_stock,
                        "promo_text": promo_text,
                        "sku": sku,
                        "rating": None,
                        "description": description
                    }
                    
                    valid_products.append(product_dict)
                    new_products[key] = product_dict
            
            print(f"  [INFO] Found {len(valid_products)} new products on page {page_num} (total for category: {len(new_products)})")
            
            if valid_products:
                current_products = load_existing_products(output_json)
                for key, product in new_products.items():
                    current_products[key] = product
                save_products(output_json, current_products)
                print(f"  [INFO] Appended {len(valid_products)} products to JSON (total: {len(current_products)})")
            
            if not valid_products and page_num > 1:
                print(f"  [INFO] No more products found, stopping pagination")
                break
            
            if len(new_products) >= max_products_per_category:
                print(f"[INFO] Reached maximum of {max_products_per_category} products for this category")
                break
            
            page_num += 1
            time.sleep(1)
            
        except PlaywrightTimeoutError:
            print(f"  [WARN] Timeout on page {page_num}, moving to next category")
            break
        except Exception as e:
            print(f"  [ERROR] Error on page {page_num}: {e}")
            import traceback
            traceback.print_exc()
            break
    
    return new_products


def monitor_keyboard():
    """Monitor keyboard input in background thread."""
    global skip_category_flag, quit_flag
    
    while True:
        try:
            try:
                line = input().strip().lower()
                if line == 's':
                    skip_category_flag.set()
                    print("\n[INFO] ✓ Skip signal received! Moving to next category...")
                elif line == 'q':
                    quit_flag.set()
                    skip_category_flag.set()
                    print("\n[INFO] ✓ Quit signal received! Exiting after current category...")
                    break
            except (EOFError, KeyboardInterrupt):
                break
        except Exception as e:
            time.sleep(0.1)
            continue


def main():
    """Main function to scrape all food categories from unimarc.cl."""
    global skip_category_flag
    
    output_json = "products.json"
    max_products_per_category = 300
    
    existing_products = load_existing_products(output_json)
    print(f"[INFO] Loaded {len(existing_products)} existing products")
    
    print("\n" + "="*60)
    print("[INFO] Keyboard controls enabled:")
    print("  - Press 's' + Enter: Skip current category and move to next")
    print("  - Press 'q' + Enter: Quit scraper after current category")
    print("="*60 + "\n")
    
    keyboard_thread = threading.Thread(target=monitor_keyboard, daemon=True)
    keyboard_thread.start()
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
            )
        )
        
        try:
            categories = discover_food_categories(page)
            
            all_new_products = {}
            for category_url in categories:
                if quit_flag.is_set():
                    print("\n[INFO] Quit signal received. Exiting...")
                    break
                
                try:
                    category_name = category_url.split("/")[-1] or category_url.split("/")[-2] or "unknown"
                    
                    new_products = scrape_category_products_playwright(
                        page, category_url, category_name, 
                        {**existing_products, **all_new_products},
                        output_json,
                        max_products_per_category
                    )
                    
                    all_new_products.update(new_products)
                    
                    if new_products:
                        merged_products = {**existing_products, **all_new_products}
                        save_products(output_json, merged_products)
                        print(f"[INFO] Category complete. Total products so far: {len(merged_products)}")
                    
                    time.sleep(1)
                    
                except Exception as e:
                    print(f"[ERROR] Error scraping category {category_url}: {e}")
                    continue
            
            final_products = {**existing_products, **all_new_products}
            save_products(output_json, final_products)
            print(f"\n[SUCCESS] Scraping complete! Total products: {len(final_products)}")
            
        finally:
            browser.close()


if __name__ == "__main__":
    main()

