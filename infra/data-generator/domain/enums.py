"""Enums – SRP: centralize constant lists used across services."""

PAYMENT_METHODS = ["CARD", "APPLE_PAY", "PAYPAL"]
PAYMENT_STATUS = ["PENDING", "SETTLED", "FAILED"]
CARRIERS = ["UPS", "DHL", "FEDEX"]
CURRENCIES = ["USD", "EUR", "GBP"]
COUNTRIES = ["US", "DE", "GB", "FR", "IL", "ES", "IT", "NL"]
CATEGORIES = ["Electronics", "Home", "Sports", "Fashion", "Books", "Beauty"]
CUSTOMER_SEGMENTS = ["VIP", "Regular", "New", "Churned"]
INTERACTION_TYPES = [
    "PAGE_VIEW",
    "SEARCH",
    "CART_ADD",
    "CART_REMOVE",
    "WISHLIST_ADD",
    "REVIEW",
    "SUPPORT_CHAT",
]
INVENTORY_CHANGE_TYPES = ["RESTOCK", "SALE", "DAMAGE", "RETURN", "TRANSFER", "ADJUSTMENT"]
BROWSERS = ["Chrome/120.0", "Firefox/121.0", "Safari/17.0", "Edge/120.0"]
OS_LIST = ["Windows NT 10.0", "Macintosh; Intel Mac OS X 10_15_7", "X11; Linux x86_64"]