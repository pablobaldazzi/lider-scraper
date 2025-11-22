#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time
import os
import sys
import threading
from typing import List, Dict, Set
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# Global flag for skipping current category
skip_category_flag = threading.Event()
quit_flag = threading.Event()


def load_existing_products(output_json: str) -> Dict[str, Dict]:
    """Load existing products from JSON file to avoid duplicates."""
    if os.path.exists(output_json):
        try:
            with open(output_json, "r", encoding="utf-8") as f:
                products = json.load(f)
                # Convert list to dict keyed by product URL or ID for fast lookup
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
    """Discover all food category URLs from jumbo.cl."""
    # Common food category URLs for jumbo.cl
    fallback_categories = [
        "https://www.jumbo.cl/carnes-y-pescados",
        "https://www.jumbo.cl/frutas-y-verduras",
        "https://www.jumbo.cl/lacteos-huevos-y-congelados",
        "https://www.jumbo.cl/quesos-y-fiambres",
        "https://www.jumbo.cl/despensa",
        "https://www.jumbo.cl/panaderia-y-pasteleria",
        "https://www.jumbo.cl/licores-bebidas-y-aguas",
        "https://www.jumbo.cl/chocolates-galletas-y-snacks"
    ]
    
    print(f"[INFO] Using {len(fallback_categories)} food categories from fallback list")
    return fallback_categories


def scrape_product_detail(page, product_url: str) -> Dict:
    """Scrape detailed product information from product detail page."""
    try:
        page.goto(product_url, wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(2000)
        
        product_detail = page.evaluate(
            """
            () => {
                const cleanText = (el) => el ? el.textContent.replace(/\\s+/g, " ").trim() : null;
                
                // Try various selectors for product description
                const descSelectors = [
                    '[data-testid="product-description"]',
                    '.product-description',
                    '.vtex-product-description-0-x-productDescriptionText',
                    '[class*="description"]',
                    '[class*="Description"]',
                    '.product-info p',
                    '.product-details p'
                ];
                
                let description = null;
                for (const selector of descSelectors) {
                    const el = document.querySelector(selector);
                    if (el && el.textContent.trim()) {
                        description = cleanText(el);
                        break;
                    }
                }
                
                // If no description found, try to get from meta tags
                if (!description) {
                    const metaDesc = document.querySelector('meta[name="description"]');
                    if (metaDesc) {
                        description = metaDesc.getAttribute('content');
                    }
                }
                
                // Get additional product info
                const brandEl = document.querySelector('[data-testid="product-brand"]') ||
                               document.querySelector('.product-brand') ||
                               document.querySelector('[class*="brand"]');
                
                const skuEl = document.querySelector('[data-testid="product-sku"]') ||
                             document.querySelector('.product-sku') ||
                             document.querySelector('[class*="sku"]');
                
                return {
                    description: description,
                    brand: cleanText(brandEl),
                    sku: cleanText(skuEl)
                };
            }
            """
        )
        
        return product_detail
    except Exception as e:
        print(f"[WARN] Could not scrape product detail from {product_url}: {e}")
        return {}


def monitor_keyboard():
    """Monitor keyboard input in background thread. Press 's' to skip current category, 'q' to quit."""
    global skip_category_flag, quit_flag
    
    while True:
        try:
            # Use blocking input - this will wait for Enter key
            # We run this in a separate thread so it doesn't block the main scraping
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
            # If keyboard monitoring fails, just continue
            time.sleep(0.1)
            continue


def scrape_category_products(page, category_url: str, category_name: str, existing_products: Dict[str, Dict], output_json: str) -> Dict[str, Dict]:
    """Scrape all products from a category with pagination."""
    global skip_category_flag
    
    print(f"\n[INFO] Scraping category: {category_name} ({category_url})")
    print(f"[INFO] 💡 Tip: Type 's' + Enter to skip this category, 'q' + Enter to quit")
    new_products = {}
    page_num = 1
    max_pages = 50  # Safety limit
    
    while page_num <= max_pages:
        # Check if skip flag is set
        if skip_category_flag.is_set():
            print(f"\n[INFO] Skipping category: {category_name}")
            skip_category_flag.clear()  # Reset flag for next category
            break
        
        try:
            # Construct paginated URL
            if "?" in category_url:
                url = f"{category_url}&page={page_num}"
            else:
                url = f"{category_url}?page={page_num}"
            
            print(f"  [INFO] Loading page {page_num}...")
            page.goto(url, wait_until="networkidle", timeout=30000)
            page.wait_for_timeout(3000)
            
            # Scroll to load lazy-loaded content
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(2000)
            
            # Extract products from current page using actual jumbo.cl HTML structure
            products = page.evaluate(
                """
                () => {
                    // Find product cards - they have data-cnstrc-item-name attribute
                    const cards = document.querySelectorAll(
                        '[data-cnstrc-item-name], ' +
                        '.border.rounded-t-lg[data-cnstrc-item-name], ' +
                        'div[data-cnstrc-item-id]'
                    );

                    const cleanText = (el) =>
                        el ? el.textContent.replace(/\\s+/g, " ").trim() : null;

                    const normalizePrice = (text) => {
                        if (!text) return null;
                        const digits = text.replace(/[^\\d]/g, "");
                        return digits ? Number(digits) : null;
                    };

                    return Array.from(cards).map(card => {
                        // Get data attributes
                        const itemId = card.getAttribute("data-cnstrc-item-id");
                        const itemName = card.getAttribute("data-cnstrc-item-name");
                        const itemPrice = card.getAttribute("data-cnstrc-item-price");
                        
                        // Product name from h2.product-card-name
                        const nameEl = card.querySelector("h2.product-card-name") ||
                                      card.querySelector("h2[class*='product-card-name']") ||
                                      card.querySelector("h2");
                        
                        // Brand from p.text-sm.text-gray-500 (the brand text)
                        const brandEl = card.querySelector("p.text-sm.text-gray-500") ||
                                       card.querySelector("p.text-gray-500");
                        
                        // Price from the bold text-lg element (main price)
                        const priceEl = card.querySelector(".text-lg.leading-5") ||
                                      card.querySelector("[class*='text-lg'][class*='font-bold']") ||
                                      card.querySelector(".flex.items-baseline .font-bold");
                        
                        // Original price (crossed out)
                        const priceOriginalEl = card.querySelector("span.line-through") ||
                                               card.querySelector(".line-through");
                        
                        // Unit price (per kg) from ppum-price-container
                        const unitPriceEl = card.querySelector(".ppum-price-container span") ||
                                           card.querySelector("[class*='ppum-price']");
                        
                        // Promo text from bg-bgflagoferta or similar
                        const promoEl = card.querySelector(".bg-bgflagoferta") ||
                                       card.querySelector("[class*='flagoferta']") ||
                                       card.querySelector("[class*='promo']");
                        
                        // Image
                        const imgEl = card.querySelector("img[src*='jumbocl.vteximg.com.br']") ||
                                     card.querySelector("img[src*='vteximg']") ||
                                     card.querySelector("img");
                        
                        // Product URL from anchor tag
                        const linkEl = card.querySelector("a[href*='/p']") ||
                                      card.querySelector("a[href]");
                        
                        const productUrl = linkEl ? 
                            (linkEl.href.startsWith('http') ? linkEl.href : 'https://www.jumbo.cl' + linkEl.href) : 
                            null;
                        
                        // Rating
                        const ratingEl = card.querySelector(".average-quantity") ||
                                      card.querySelector("[class*='average-quantity']");
                        
                        // Extract price from text (remove $ and dots)
                        let price = null;
                        if (itemPrice) {
                            price = normalizePrice(itemPrice);
                        } else if (priceEl) {
                            const priceText = cleanText(priceEl);
                            // Remove crossed out price if present
                            const priceWithoutStrike = priceText ? priceText.split('$').filter(p => p.trim()).pop() : null;
                            price = normalizePrice(priceWithoutStrike || priceText);
                        }
                        
                        // Extract unit price
                        let unitPrice = null;
                        if (unitPriceEl) {
                            const unitText = cleanText(unitPriceEl);
                            // Extract price per kg (format: "$10.843 x kg")
                            if (unitText && unitText.includes('x kg')) {
                                unitPrice = normalizePrice(unitText.split('x')[0]);
                            }
                        }
                        
                        return {
                            id: itemId || (productUrl ? productUrl.split('/').pop() : null),
                            name: cleanText(nameEl) || itemName || null,
                            brand: cleanText(brandEl),
                            category: null,  // Will be filled later
                            price: price,
                            price_original: normalizePrice(cleanText(priceOriginalEl)),
                            currency: "CLP",
                            size: null,  // Size info might be in name
                            unit_price: unitPrice,
                            image_url: imgEl ? (imgEl.src || imgEl.getAttribute("data-src") || imgEl.getAttribute("src")) : null,
                            product_url: productUrl,
                            in_stock: true,  // Assume in stock if shown
                            promo_text: cleanText(promoEl),
                            sku: itemId || null,
                            rating: ratingEl ? parseFloat(cleanText(ratingEl)) : null,
                            description: null  // Will be filled from detail page
                        };
                    });
                }
                """
            )
            
            # Filter valid products and check for duplicates
            valid_products = []
            for p in products:
                if p.get("name") and (p.get("price") is not None or p.get("product_url")):
                    key = p.get("product_url") or p.get("id")
                    if key and key not in existing_products and key not in new_products:
                        p["category"] = category_name
                        valid_products.append(p)
                        new_products[key] = p
            
            print(f"  [INFO] Found {len(valid_products)} new products on page {page_num}")
            
            # Continuously append to JSON after each page
            if valid_products:
                # Reload existing products to get latest state
                current_products = load_existing_products(output_json)
                # Merge new products
                for key, product in new_products.items():
                    current_products[key] = product
                # Save immediately
                save_products(output_json, current_products)
                print(f"  [INFO] Appended {len(valid_products)} products to JSON (total: {len(current_products)})")
            
            # Check if there are more pages
            has_next = page.evaluate(
                """
                () => {
                    // Try various selectors for next button
                    const selectors = [
                        '[aria-label*="siguiente"]',
                        '[aria-label*="next"]',
                        '.pagination-next',
                        '[class*="next"]',
                        'button[aria-label*="Siguiente"]',
                        'a[aria-label*="Siguiente"]'
                    ];
                    
                    for (const selector of selectors) {
                        const nextButton = document.querySelector(selector);
                        if (nextButton && !nextButton.disabled && 
                            !nextButton.classList.contains('disabled') &&
                            !nextButton.hasAttribute('disabled')) {
                            return true;
                        }
                    }
                    
                    // Also check if button text contains "siguiente" or "next"
                    const buttons = document.querySelectorAll('button, a');
                    for (const btn of buttons) {
                        const text = btn.textContent.toLowerCase();
                        if ((text.includes('siguiente') || text.includes('next')) &&
                            !btn.disabled && !btn.classList.contains('disabled')) {
                            return true;
                        }
                    }
                    
                    return false;
                }
                """
            )
            
            # Also check if we got products (if no products, probably no more pages)
            if not valid_products and page_num > 1:
                print(f"  [INFO] No more products found, stopping pagination")
                break
            
            if not has_next:
                # Try to detect if we're on last page by checking page numbers
                page_numbers = page.evaluate(
                    """
                    () => {
                        const currentPage = document.querySelector('.pagination-current, [class*="current"], [aria-current="page"]');
                        const totalPages = document.querySelectorAll('.pagination-page, [class*="page"]').length;
                        return { current: currentPage ? parseInt(currentPage.textContent) : null, total: totalPages };
                    }
                    """
                )
                if page_numbers.get("current") and page_numbers.get("total") and page_num >= page_numbers.get("total", 1):
                    break
            
            page_num += 1
            time.sleep(0.1)  # Be respectful with requests
            
        except PlaywrightTimeoutError:
            print(f"  [WARN] Timeout on page {page_num}, moving to next category")
            break
        except Exception as e:
            print(f"  [ERROR] Error on page {page_num}: {e}")
            break
    
    
    return new_products


def main():
    """Main function to scrape all food categories from jumbo.cl."""
    global skip_category_flag
    
    output_json = "jumbo_products.json"
    
    # Load existing products
    existing_products = load_existing_products(output_json)
    print(f"[INFO] Loaded {len(existing_products)} existing products")
    
    # Start keyboard monitoring thread
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
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
            )
        )
        
        try:
            # Discover all food categories
            categories = discover_food_categories(page)
            
            # Scrape each category
            all_new_products = {}
            for category_url in categories:
                # Check if quit flag is set
                if quit_flag.is_set():
                    print("\n[INFO] Quit signal received. Exiting...")
                    break
                
                try:
                    # Extract category name from URL
                    category_name = category_url.split("/")[-1] or category_url.split("/")[-2] or "unknown"
                    
                    # Scrape products from this category
                    # Note: JSON is already being saved continuously inside scrape_category_products
                    new_products = scrape_category_products(
                        page, category_url, category_name, 
                        {**existing_products, **all_new_products},
                        output_json
                    )
                    
                    # Merge new products
                    all_new_products.update(new_products)
                    
                    # Final save for this category (already saved during scraping, but ensure consistency)
                    if new_products:
                        merged_products = {**existing_products, **all_new_products}
                        save_products(output_json, merged_products)
                        print(f"[INFO] Category complete. Total products so far: {len(merged_products)}")
                    
                    time.sleep(0.1)  # Be respectful between categories
                    
                except Exception as e:
                    print(f"[ERROR] Error scraping category {category_url}: {e}")
                    continue
            
            # Final save
            final_products = {**existing_products, **all_new_products}
            save_products(output_json, final_products)
            print(f"\n[SUCCESS] Scraping complete! Total products: {len(final_products)}")
            
        finally:
            browser.close()


if __name__ == "__main__":
    main()
