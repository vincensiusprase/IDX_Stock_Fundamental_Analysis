# @title 📊 IDXHEALTH VALUATION v4

import yfinance as yf
import pandas as pd
import numpy as np
from google.colab import auth
import gspread
from google.auth import default
from gspread_dataframe import set_with_dataframe
from datetime import datetime, timedelta

# ════════════════════════════════════════════════════════════════
# 1. OTENTIKASI
# ════════════════════════════════════════════════════════════════
print("Mohon lakukan otentikasi...")
auth.authenticate_user()
creds, _ = default()
gc = gspread.authorize(creds)
print("✅ Otentikasi Berhasil!\n")

# ════════════════════════════════════════════════════════════════
# 2. KONFIGURASI
# ════════════════════════════════════════════════════════════════
DAFTAR_SAHAM = [
        "BMHS.JK", "CARE.JK", "CHEK.JK", "DGNS.JK", "DKHH.JK", "DVLA.JK", "HALO.JK", "HEAL.JK", "IKPM.JK", "INAF.JK",
        "IRRA.JK", "KAEF.JK", "KLBF.JK", "LABS.JK", "MDLA.JK", "MEDS.JK", "MERK.JK", "MIKA.JK", "MMIX.JK", "MTMH.JK",
        "OBAT.JK", "OMED.JK", "PEHA.JK", "PEVE.JK", "PRDA.JK", "PRAY.JK", "PRIM.JK", "PYFA.JK", "RSCH.JK", "RSGK.JK",
        "SAME.JK", "SCPI.JK", "SIDO.JK", "SILO.JK", "SOHO.JK", "SRAJ.JK", "SURI.JK", "TSPC.JK"
]

SPREADSHEET_ID  = '1X2lQYfPC6Dj8WEwbJsXevZgprXw2f2pi_obpTUZFFn8'
SHEET_VALUATION = 'IDXHEALTH_VALUATION'
SHEET_DETAILS   = 'IDXHEALTHDetails'

# --- DCF Parameters ---
RISK_FREE_RATE   = 0.065
EQUITY_RISK_PREM = 0.055
TERMINAL_GROWTH  = 0.045
DCF_YEARS        = 5
WACC_MIN         = 0.08
WACC_MAX         = 0.20

# --- Fair Value Weights ---
WEIGHT_DCF       = 0.50
WEIGHT_GRAHAM    = 0.30
WEIGHT_EV_EBITDA = 0.20

# --- Signal Thresholds ---
MOS_MURAH        = 0.20
MOS_MAHAL        = -0.10

# ════════════════════════════════════════════════════════════════
# 3. MAPPING yfinance → INTERNAL
# ════════════════════════════════════════════════════════════════
# Key = nama PERSIS dari yfinance (info['sector'] / info['industry'])
# Value = nama internal yang dipakai di GRAHAM_CONSTANTS

SECTOR_MAP = {
    "Technology":                "Technology",
    "Healthcare":                "Healthcare",
    "Financial Services":        "Financial Services",
    "Consumer Defensive":        "Consumer Defensive",
    "Consumer Cyclical":         "Consumer Cyclical",
    "Communication Services":    "Industrials",       # Telco masuk Industrials di IDX
    "Utilities":                 "Utilities",
    "Industrials":               "Industrials",
    "Basic Materials":           "Basic Materials",
    "Energy":                    "Energy",
    "Real Estate":               "Real Estate",
}

INDUSTRY_MAP = {
    # ── Technology ──────────────────────────────────────────────
    "Software—Application":                        "Software",
    "Software—Infrastructure":                     "Software",
    "Information Technology Services":             "Software",
    "Internet Content & Information":              "Software",
    "Computer Hardware":                           "Computer Hardware",
    "Electronic Components":                       "Computer Hardware",
    "Electronics & Computer Distribution":         "Computer Hardware",
    "Semiconductors":                              "Computer Hardware",
    "Communication Equipment":                     "Computer Hardware",
    "Consumer Electronics":                        "Computer Hardware",
    "Scientific & Technical Instruments":          "Computer Hardware",

    # ── Healthcare ───────────────────────────────────────────────
    "Drug Manufacturers—General":                  "Healthcare",
    "Drug Manufacturers—Specialty & Generic":      "Healthcare",
    "Pharmaceutical Retailers":                    "Healthcare",
    "Biotechnology":                               "Healthcare",
    "Medical Devices":                             "Healthcare",
    "Medical Distribution":                        "Healthcare",
    "Medical Instruments & Supplies":              "Healthcare",
    "Health Information Services":                 "Healthcare",
    "Healthcare Plans":                            "Healthcare",
    "Medical Care Facilities":                     "Healthcare",
    "Diagnostics & Research":                      "Healthcare",

    # ── Financial Services ───────────────────────────────────────
    "Banks—Regional":                              "Banks",
    "Banks—Diversified":                           "Banks",
    "Insurance—Life":                              "Insurance",
    "Insurance—Property & Casualty":               "Insurance",
    "Insurance—Diversified":                       "Insurance",
    "Insurance—Reinsurance":                       "Insurance",
    "Insurance—Specialty":                         "Insurance",
    "Credit Services":                             "Credit Services",
    "Financial Conglomerates":                     "Conglomerates",
    "Capital Markets":                             "Asset Management",
    "Asset Management":                            "Asset Management",
    "Investment—Banking & Investment Services":    "Asset Management",
    "Financial Data & Stock Exchanges":            "Asset Management",
    "Mortgage Finance":                            "Credit Services",

    # ── Consumer Defensive ───────────────────────────────────────
    "Tobacco":                                     "Tobacco",
    "Household & Personal Products":               "Household & Personal Products",
    "Packaged Foods":                              "Packaged Foods",
    "Confectioners":                               "Packaged Foods",
    "Beverages—Non-Alcoholic":                     "Packaged Foods",
    "Beverages—Alcoholic":                         "Packaged Foods",
    "Beverages—Brewers":                           "Packaged Foods",
    "Farm Products":                               "Packaged Foods",
    "Food Distribution":                           "Packaged Foods",
    "Grocery Stores":                              "Household & Personal Products",
    "Discount Stores":                             "Household & Personal Products",

    # ── Consumer Cyclical ────────────────────────────────────────
    "Broadcasting":                                "Entertainment",
    "Entertainment":                               "Entertainment",
    "Publishing":                                  "Entertainment",
    "Advertising Agencies":                        "Entertainment",
    "Leisure":                                     "Leisure Products",
    "Travel Services":                             "Restaurants / Travel Services",
    "Gambling":                                    "Leisure Products",
    "Specialty Retail":                            "Specialty Retail",
    "Department Stores":                           "Specialty Retail",
    "Home Improvement Retail":                     "Specialty Retail",
    "Luxury Goods":                                "Apparel Retail",
    "Apparel Retail":                              "Apparel Retail",
    "Apparel Manufacturing":                       "Apparel Retail",
    "Footwear & Accessories":                      "Apparel Retail",
    "Auto Manufacturers":                          "Auto Manufacturers",
    "Auto Parts":                                  "Auto Manufacturers",
    "Auto & Truck Dealerships":                    "Auto Manufacturers",
    "Furnishings, Fixtures & Appliances":          "Furnishings / Appliances",
    "Residential Construction":                    "Furnishings / Appliances",
    "Personal Services":                           "Restaurants / Travel Services",
    "Restaurants":                                 "Restaurants / Travel Services",
    "Hotels & Motels":                             "Restaurants / Travel Services",

    # ── Industrials (termasuk Infrastruktur IDX) ─────────────────
    "Telecom Services":                            "Telecom Services",
    "Airport & Air Services":                      "Railroads / Logistics",
    "Toll Roads":                                  "Railroads / Logistics",
    "Ports & Services":                            "Railroads / Logistics",
    "Railroads":                                   "Airlines / Railroads / Marine",
    "Airlines":                                    "Airlines / Railroads / Marine",
    "Marine Shipping":                             "Airlines / Railroads / Marine",
    "Trucking":                                    "Integrated Freight & Logistics",
    "Integrated Freight & Logistics":              "Integrated Freight & Logistics",
    "Courier & Delivery":                          "Integrated Freight & Logistics",
    "Engineering & Construction":                  "Engineering & Construction",
    "Infrastructure Operations":                   "Railroads / Logistics",
    "Conglomerates":                               "Conglomerates",
    "Specialty Industrial Machinery":              "Industrial Products",
    "Industrial Distribution":                     "Industrial Products",
    "Electrical Equipment & Parts":                "Industrial Products",
    "Metal Fabrication":                           "Industrial Products",
    "Tools & Accessories":                         "Industrial Products",
    "Staffing & Employment Services":              "Engineering & Construction",
    "Waste Management":                            "Engineering & Construction",
    "Security & Protection Services":              "Engineering & Construction",
    "Rental & Leasing Services":                   "Engineering & Construction",
    "Business Equipment & Supplies":               "Industrial Products",
    "Consulting Services":                         "Engineering & Construction",

    # ── Utilities ────────────────────────────────────────────────
    "Utilities—Regulated Electric":                "Utilities",
    "Utilities—Regulated Gas":                     "Utilities",
    "Utilities—Regulated Water":                   "Utilities",
    "Utilities—Independent Power Producers":       "Utilities",
    "Utilities—Renewable":                         "Renewable",
    "Utilities—Diversified":                       "Utilities",

    # ── Energy ───────────────────────────────────────────────────
    "Oil & Gas Integrated":                        "Oil & Gas",
    "Oil & Gas E&P":                               "Oil & Gas",
    "Oil & Gas Midstream":                         "Oil & Gas",
    "Oil & Gas Refining & Marketing":              "Oil & Gas",
    "Oil & Gas Equipment & Services":              "Oil & Gas",
    "Thermal Coal":                                "Thermal Coal",
    "Coking Coal":                                 "Thermal Coal",
    "Solar":                                       "Renewable",

    # ── Basic Materials ──────────────────────────────────────────
    "Steel":                                       "Basic Materials",
    "Aluminum":                                    "Basic Materials",
    "Copper":                                      "Basic Materials",
    "Gold":                                        "Basic Materials",
    "Silver":                                      "Basic Materials",
    "Specialty Chemicals":                         "Basic Materials",
    "Chemicals":                                   "Basic Materials",
    "Paper & Paper Products":                      "Basic Materials",
    "Lumber & Wood Production":                    "Basic Materials",
    "Agricultural Inputs":                         "Basic Materials",
    "Building Materials":                          "Basic Materials",
    "Cement":                                      "Basic Materials",
    "Other Industrial Metals & Mining":            "Basic Materials",

    # ── Real Estate ──────────────────────────────────────────────
    "Real Estate—Development":                     "Real Estate",
    "Real Estate—Diversified":                     "Real Estate",
    "Real Estate Services":                        "Real Estate",
    "REIT—Diversified":                            "Real Estate",
    "REIT—Office":                                 "Real Estate",
    "REIT—Retail":                                 "Real Estate",
    "REIT—Industrial":                             "Real Estate",
    "REIT—Residential":                            "Real Estate",
}

def normalize_sector(raw):   return SECTOR_MAP.get(raw, raw)
def normalize_industry(raw): return INDUSTRY_MAP.get(raw, raw)

# ════════════════════════════════════════════════════════════════
# 4. GRAHAM CONSTANTS — key pakai nama yfinance yang sudah di-normalize
# ════════════════════════════════════════════════════════════════
# Format: (normalized_sector, normalized_industry): {K, target_pe, target_pb}

# Fallback Graham K jika sector/industry tidak ditemukan (Benjamin Graham original)
GRAHAM_K_DEFAULT = 22.5

GRAHAM_CONSTANTS = {
    # ── Consumer Defensive ───────────────────────────────────────
    ("Consumer Defensive", "Tobacco"):                              {"K": 35.6608, "target_pe": 17.92, "target_pb": 1.99},
    ("Consumer Defensive", "Household & Personal Products"):        {"K": 97.5664, "target_pe": 16.88, "target_pb": 5.78},
    ("Consumer Defensive", "Packaged Foods"):                       {"K": 33.6000, "target_pe": 12.00, "target_pb": 2.80},

    # ── Consumer Cyclical ────────────────────────────────────────
    ("Consumer Cyclical",  "Entertainment"):                        {"K": 28.2880, "target_pe": 10.40, "target_pb": 2.72},
    ("Consumer Cyclical",  "Specialty Retail"):                     {"K": 13.3986, "target_pe":  9.78, "target_pb": 1.37},
    ("Consumer Cyclical",  "Apparel Retail"):                       {"K":  8.3239, "target_pe":  3.37, "target_pb": 2.47},
    ("Consumer Cyclical",  "Auto Manufacturers"):                   {"K": 28.2880, "target_pe": 10.40, "target_pb": 2.72},
    ("Consumer Cyclical",  "Leisure Products"):                     {"K": 28.2880, "target_pe": 10.40, "target_pb": 2.72},
    ("Consumer Cyclical",  "Furnishings / Appliances"):             {"K": 28.2880, "target_pe": 10.40, "target_pb": 2.72},
    ("Consumer Cyclical",  "Restaurants / Travel Services"):        {"K": 28.2880, "target_pe": 10.40, "target_pb": 2.72},

    # ── Technology ───────────────────────────────────────────────
    ("Technology",         "Software"):                             {"K": 66.4598, "target_pe": 17.77, "target_pb": 3.74},
    ("Technology",         "Computer Hardware"):                    {"K": 33.8800, "target_pe":  8.80, "target_pb": 3.85},

    # ── Healthcare ───────────────────────────────────────────────
    ("Healthcare",         "Healthcare"):                           {"K": 15.2448, "target_pe":  7.94, "target_pb": 1.92},
    # Rumah Sakit (Jasa & Peralatan Kesehatan) — K lebih tinggi
    # yfinance biasanya masuk "Medical Care Facilities" lalu di-normalize ke "Healthcare"
    # Jika perlu split, tambahkan di sini dengan industry berbeda

    # ── Financial Services ───────────────────────────────────────
    ("Financial Services", "Banks"):                                {"K": 16.5163, "target_pe":  9.89, "target_pb": 1.67},
    ("Financial Services", "Insurance"):                            {"K": 10.3375, "target_pe":  8.27, "target_pb": 1.25},
    ("Financial Services", "Credit Services"):                      {"K": 10.3964, "target_pe": 11.06, "target_pb": 0.94},
    ("Financial Services", "Asset Management"):                     {"K": 127.344, "target_pe": 37.90, "target_pb": 3.36},
    ("Financial Services", "Conglomerates"):                        {"K": 79.9968, "target_pe": 19.23, "target_pb": 4.16},

    # ── Industrials ──────────────────────────────────────────────
    ("Industrials",        "Conglomerates"):                        {"K": 15.8841, "target_pe":  9.99, "target_pb": 1.59},
    ("Industrials",        "Industrial Products"):                  {"K":  8.9090, "target_pe":  7.55, "target_pb": 1.18},
    ("Industrials",        "Engineering & Construction"):           {"K":  7.6120, "target_pe":  6.92, "target_pb": 1.10},
    ("Industrials",        "Railroads / Logistics"):                {"K": 11.9880, "target_pe": 10.80, "target_pb": 1.11},
    ("Industrials",        "Telecom Services"):                     {"K": 32.8659, "target_pe": 15.43, "target_pb": 2.13},
    ("Industrials",        "Airlines / Railroads / Marine"):        {"K":  0.5856, "target_pe":  1.83, "target_pb": 0.32},
    ("Industrials",        "Integrated Freight & Logistics"):       {"K": 31.4424, "target_pe": 15.88, "target_pb": 1.98},

    # ── Utilities ────────────────────────────────────────────────
    ("Utilities",          "Utilities"):                            {"K": 36.6085, "target_pe": 17.35, "target_pb": 2.11},
    ("Utilities",          "Renewable"):                            {"K": 71.1657, "target_pe": 22.17, "target_pb": 3.21},

    # ── Energy ───────────────────────────────────────────────────
    ("Energy",             "Oil & Gas"):                            {"K": 71.1657, "target_pe": 22.17, "target_pb": 3.21},
    ("Energy",             "Thermal Coal"):                         {"K": 71.1657, "target_pe": 22.17, "target_pb": 3.21},
    ("Energy",             "Renewable"):                            {"K": 71.1657, "target_pe": 22.17, "target_pb": 3.21},

    # ── Basic Materials ──────────────────────────────────────────
    ("Basic Materials",    "Basic Materials"):                      {"K": 48.0150, "target_pe": 16.50, "target_pb": 2.91},

    # ── Real Estate ──────────────────────────────────────────────
    ("Real Estate",        "Real Estate"):                          {"K": 21.5775, "target_pe": 15.75, "target_pb": 1.37},
}

# EV/EBITDA target multiple — key pakai normalized sector
EV_EBITDA_TARGETS = {
    "Technology":           18.0,
    "Healthcare":           14.0,
    "Consumer Defensive":   12.0,
    "Consumer Cyclical":    10.0,
    "Financial Services":    9.0,
    "Energy":                6.0,
    "Basic Materials":       7.0,
    "Industrials":           8.0,
    "Utilities":             8.0,
    "Real Estate":           9.0,
}
EV_EBITDA_DEFAULT = 9.0

# EV/Sales (Price-to-Sales) target multiple — fallback jika FCF tidak cukup positif
# Dipakai untuk growth/loss-making companies yang tidak bisa di-DCF
EV_SALES_TARGETS = {
    "Technology":          6.0,   # SaaS/Software premium
    "Healthcare":          4.0,
    "Consumer Defensive":  2.0,
    "Consumer Cyclical":   1.5,
    "Financial Services":  3.0,
    "Energy":              1.0,
    "Basic Materials":     1.0,
    "Industrials":         1.2,
    "Utilities":           2.0,
    "Real Estate":         3.0,
}
EV_SALES_DEFAULT = 2.0

# Fallback Graham K jika sektor/industri tidak ditemukan di GRAHAM_CONSTANTS
# Menggunakan konstanta asli Benjamin Graham
GRAHAM_K_DEFAULT  = 22.5

# ════════════════════════════════════════════════════════════════
# 5. SCORING CONFIG — bobot rekomendasi Claude (total = 100%)
# ════════════════════════════════════════════════════════════════
# stock_type: "bank" | "karya" | "umum"
# Ditentukan otomatis dari yfinance sector + industry

def get_stock_type(sector, industry):
    """
    bank  → Financial Services / Banks
    karya → Industrials / Engineering & Construction (BUMN Karya)
    bank juga → Financial Services / Credit Services, Insurance, dll
    umum  → semua lainnya
    """
    if sector == "Financial Services":
        return "bank"
    if sector == "Industrials" and industry == "Engineering & Construction":
        return "karya"
    return "umum"

# Definisi scoring: setiap entry = satu rasio yang dinilai
# format: {
#   "rasio"     : nama kolom di data,
#   "label"     : nama tampilan,
#   "tipe"      : "umum" | "bank" | "karya" | "semua" | list tipe,
#   "bobot_umum": float (0-1),  # bobot untuk tipe umum
#   "bobot_bank": float (0-1),  # bobot untuk tipe bank (0 = tidak dipakai)
#   "skala"     : [(batas_bawah, batas_atas, skor), ...]  inklusif kiri
#                 batas_atas=None → tak terbatas ke atas
#                 urutan: dari buruk ke bagus
# }

SCORING_CONFIG = [
    # ════════════════════════════════════════════════════════
    # PROFITABILITAS
    # ════════════════════════════════════════════════════════
    {
        "rasio": "GPM %", "label": "GPM (%)", "tipe": "umum",
        "bobot_umum": 0.03, "bobot_bank": 0.00,
        "skala": [(None,10,1),(10,20,2),(20,30,3),(30,50,4),(50,None,5)],
    },
    {
        "rasio": "GPM %", "label": "GPM Karya (%)", "tipe": "karya",
        "bobot_umum": 0.05, "bobot_bank": 0.00,
        "skala": [(None,5,1),(5,10,2),(10,15,3),(15,20,4),(20,None,5)],
    },
    {
        "rasio": "OPM %", "label": "OPM (%)", "tipe": "umum",
        "bobot_umum": 0.08, "bobot_bank": 0.00,
        "skala": [(None,5,1),(5,10,2),(10,15,3),(15,25,4),(25,None,5)],
    },
    {
        "rasio": "OPM %", "label": "OPM Karya (%)", "tipe": "karya",
        "bobot_umum": 0.07, "bobot_bank": 0.00,
        "skala": [(None,3,1),(3,6,2),(6,10,3),(10,15,4),(15,None,5)],
    },
    {
        "rasio": "NPM %", "label": "NPM (%)", "tipe": ["umum","karya"],
        "bobot_umum": 0.08, "bobot_bank": 0.00,
        "skala": [(None,0,1),(0,5,2),(5,10,3),(10,15,4),(15,None,5)],
    },
    {
        "rasio": "NPM %", "label": "NPM Bank (%)", "tipe": "bank",
        "bobot_umum": 0.00, "bobot_bank": 0.08,
        "skala": [(None,10,1),(10,15,2),(15,20,3),(20,30,4),(30,None,5)],
    },
    {
        "rasio": "ROE %", "label": "ROE (%)", "tipe": ["umum","karya"],
        "bobot_umum": 0.12, "bobot_bank": 0.00,
        "skala": [(None,5,1),(5,8,2),(8,12,3),(12,20,4),(20,None,5)],
    },
    {
        "rasio": "ROE %", "label": "ROE Bank (%)", "tipe": "bank",
        "bobot_umum": 0.00, "bobot_bank": 0.12,
        "skala": [(None,5,1),(5,8,2),(8,12,3),(12,20,4),(20,None,5)],
    },
    {
        "rasio": "ROA %", "label": "ROA (%)", "tipe": ["umum","karya"],
        "bobot_umum": 0.04, "bobot_bank": 0.00,
        "skala": [(None,1,1),(1,3,2),(3,5,3),(5,10,4),(10,None,5)],
    },
    {
        "rasio": "ROA %", "label": "ROA Bank (%)", "tipe": "bank",
        "bobot_umum": 0.00, "bobot_bank": 0.08,
        "skala": [(None,0.5,1),(0.5,1.0,2),(1.0,1.5,3),(1.5,2.5,4),(2.5,None,5)],
    },
    # FCF / Net Income — kualitas laba
    {
        "rasio": "FCF_to_NI", "label": "FCF/Net Income", "tipe": ["umum","karya"],
        "bobot_umum": 0.05, "bobot_bank": 0.00,
        "skala": [(None,0,1),(0,0.3,2),(0.3,0.7,3),(0.7,1.2,4),(1.2,None,5)],
    },
    {
        "rasio": "FCF_to_NI", "label": "FCF/Net Income Bank", "tipe": "bank",
        "bobot_umum": 0.00, "bobot_bank": 0.05,
        "skala": [(None,0,1),(0,0.3,2),(0.3,0.7,3),(0.7,1.2,4),(1.2,None,5)],
    },

    # ════════════════════════════════════════════════════════
    # SOLVABILITAS
    # ════════════════════════════════════════════════════════
    {
        "rasio": "DER (x)", "label": "DER (x)", "tipe": "umum",
        "bobot_umum": 0.06, "bobot_bank": 0.00,
        "skala": [(2.0,None,1),(1.5,2.0,2),(1.0,1.5,3),(0.5,1.0,4),(None,0.5,5)],
        "inverse": True,
    },
    {
        "rasio": "DER (x)", "label": "DER Karya (x)", "tipe": "karya",
        "bobot_umum": 0.06, "bobot_bank": 0.00,
        "skala": [(4.0,None,1),(3.0,4.0,2),(2.0,3.0,3),(1.0,2.0,4),(None,1.0,5)],
        "inverse": True,
    },
    {
        "rasio": "DAR (x)", "label": "DAR (x)", "tipe": ["umum","karya"],
        "bobot_umum": 0.02, "bobot_bank": 0.00,
        "skala": [(0.8,None,1),(0.6,0.8,2),(0.4,0.6,3),(0.2,0.4,4),(None,0.2,5)],
        "inverse": True,
    },
    {
        "rasio": "Interest_Coverage", "label": "Interest Cov. (x)", "tipe": "umum",
        "bobot_umum": 0.06, "bobot_bank": 0.00,
        "skala": [(None,1.0,1),(1.0,2.0,2),(2.0,5.0,3),(5.0,10.0,4),(10.0,None,5)],
    },
    {
        "rasio": "Interest_Coverage", "label": "Interest Cov. Karya (x)", "tipe": "karya",
        "bobot_umum": 0.06, "bobot_bank": 0.00,
        "skala": [(None,1.0,1),(1.0,2.0,2),(2.0,5.0,3),(5.0,10.0,4),(10.0,None,5)],
    },
    # Net Debt / EBITDA
    {
        "rasio": "NetDebt_EBITDA", "label": "Net Debt/EBITDA (x)", "tipe": "umum",
        "bobot_umum": 0.06, "bobot_bank": 0.00,
        "skala": [(5.0,None,1),(3.0,5.0,2),(2.0,3.0,3),(1.0,2.0,4),(None,1.0,5)],
        "inverse": True,
    },
    {
        "rasio": "NetDebt_EBITDA", "label": "Net Debt/EBITDA Karya (x)", "tipe": "karya",
        "bobot_umum": 0.05, "bobot_bank": 0.00,
        "skala": [(8.0,None,1),(6.0,8.0,2),(4.0,6.0,3),(2.0,4.0,4),(None,2.0,5)],
        "inverse": True,
    },
    # Risk Factor & Asset Turnover Bank
    {
        "rasio": "Risk Factor", "label": "Risk Factor", "tipe": "bank",
        "bobot_umum": 0.00, "bobot_bank": 0.17,
        "skala": [(50,None,1),(40,50,2),(30,40,3),(20,30,4),(None,20,5)],
        "inverse": True,
    },
    {
        "rasio": "Asset Turnover (x)", "label": "Asset Turnover Bank (x)", "tipe": "bank",
        "bobot_umum": 0.00, "bobot_bank": 0.17,
        "skala": [(None,0.05,1),(0.05,0.08,2),(0.08,0.12,3),(0.12,0.15,4),(0.15,None,5)],
    },

    # ════════════════════════════════════════════════════════
    # VALUASI
    # ════════════════════════════════════════════════════════
    {
        "rasio": "PER (x)", "label": "PER (x)", "tipe": ["umum","karya"],
        "bobot_umum": 0.06, "bobot_bank": 0.00,
        "skala": [(25,None,1),(20,25,2),(15,20,3),(10,15,4),(None,10,5)],
        "inverse": True,
    },
    {
        "rasio": "PER (x)", "label": "PER Bank (x)", "tipe": "bank",
        "bobot_umum": 0.00, "bobot_bank": 0.08,
        "skala": [(25,None,1),(20,25,2),(15,20,3),(10,15,4),(None,10,5)],
        "inverse": True,
    },
    {
        "rasio": "PBV (x)", "label": "PBV (x)", "tipe": ["umum","karya"],
        "bobot_umum": 0.06, "bobot_bank": 0.00,
        "skala": [(3.0,None,1),(2.0,3.0,2),(1.0,2.0,3),(0.5,1.0,4),(None,0.5,5)],
        "inverse": True,
    },
    {
        "rasio": "PBV (x)", "label": "PBV Bank (x)", "tipe": "bank",
        "bobot_umum": 0.00, "bobot_bank": 0.10,
        "skala": [(4.5,None,1),(3.5,4.5,2),(2.5,3.5,3),(1.5,2.5,4),(None,1.5,5)],
        "inverse": True,
    },
    # EV/EBITDA — recycle dari valuation, skala per sektor di-handle di scoring func
    {
        "rasio": "EV_EBITDA_score_input", "label": "EV/EBITDA (x)", "tipe": ["umum","karya"],
        "bobot_umum": 0.05, "bobot_bank": 0.00,
        "skala": [(20,None,1),(15,20,2),(10,15,3),(7,10,4),(None,7,5)],
        "inverse": True,
    },
    {
        "rasio": "Dividend Yield %", "label": "Div. Yield (%)", "tipe": ["umum","karya"],
        "bobot_umum": 0.06, "bobot_bank": 0.00,
        "skala": [(None,0,1),(0,2,2),(2,4,3),(4,6,4),(6,None,5)],
    },
    {
        "rasio": "Dividend Yield %", "label": "Div. Yield Bank (%)", "tipe": "bank",
        "bobot_umum": 0.00, "bobot_bank": 0.05,
        "skala": [(None,0,1),(0,2,2),(2,4,3),(4,6,4),(6,None,5)],
    },

    # ════════════════════════════════════════════════════════
    # PERTUMBUHAN
    # ════════════════════════════════════════════════════════
    {
        "rasio": "Revenue_CAGR3", "label": "Revenue CAGR 3Y (%)", "tipe": ["umum","karya"],
        "bobot_umum": 0.03, "bobot_bank": 0.00,
        "skala": [(None,0,1),(0,5,2),(5,10,3),(10,20,4),(20,None,5)],
    },
    {
        "rasio": "Revenue_CAGR3", "label": "Revenue CAGR 3Y Bank (%)", "tipe": "bank",
        "bobot_umum": 0.00, "bobot_bank": 0.05,
        "skala": [(None,0,1),(0,5,2),(5,10,3),(10,20,4),(20,None,5)],
    },
    {
        "rasio": "EPS_CAGR3", "label": "EPS CAGR 3Y (%)", "tipe": ["umum","karya"],
        "bobot_umum": 0.03, "bobot_bank": 0.00,
        "skala": [(None,-10,1),(-10,0,2),(0,8,3),(8,20,4),(20,None,5)],
    },
    {
        "rasio": "EPS_CAGR3", "label": "EPS CAGR 3Y Bank (%)", "tipe": "bank",
        "bobot_umum": 0.00, "bobot_bank": 0.05,
        "skala": [(None,-10,1),(-10,0,2),(0,8,3),(8,20,4),(20,None,5)],
    },

    # ════════════════════════════════════════════════════════
    # LIKUIDITAS & AKTIVITAS
    # ════════════════════════════════════════════════════════
    {
        "rasio": "Current Ratio (x)", "label": "Current Ratio (x)", "tipe": "umum",
        "bobot_umum": 0.03, "bobot_bank": 0.00,
        "skala": [(None,0.8,1),(0.8,1.0,2),(1.0,1.5,3),(1.5,2.5,4),(2.5,None,5)],
    },
    {
        "rasio": "Current Ratio (x)", "label": "Current Ratio Karya (x)", "tipe": "karya",
        "bobot_umum": 0.05, "bobot_bank": 0.00,
        "skala": [(None,1.0,1),(1.0,1.2,2),(1.2,1.5,3),(1.5,2.0,4),(2.0,None,5)],
    },
    {
        "rasio": "Inventory Turnover", "label": "Inventory TO (x)", "tipe": "umum",
        "bobot_umum": 0.03, "bobot_bank": 0.00,
        "skala": [(None,2,1),(2,3,2),(3,5,3),(5,8,4),(8,None,5)],
    },
    {
        "rasio": "Asset Turnover (x)", "label": "Asset Turnover (x)", "tipe": ["umum","karya"],
        "bobot_umum": 0.03, "bobot_bank": 0.00,
        "skala": [(None,0.3,1),(0.3,0.5,2),(0.5,0.8,3),(0.8,1.2,4),(1.2,None,5)],
    },
    {
        "rasio": "Receivables TO", "label": "Receivables TO (x)", "tipe": "umum",
        "bobot_umum": 0.02, "bobot_bank": 0.00,
        "skala": [(None,4,1),(4,6,2),(6,9,3),(9,12,4),(12,None,5)],
    },
    {
        "rasio": "Receivables TO", "label": "Receivables TO Karya (x)", "tipe": "karya",
        "bobot_umum": 0.03, "bobot_bank": 0.00,
        "skala": [(None,2,1),(2,3,2),(3,4,3),(4,6,4),(6,None,5)],
    },
    # CCC — hanya consumer sector, tipe "consumer" dihandle khusus
    {
        "rasio": "CCC", "label": "Cash Conv. Cycle (hari)", "tipe": "consumer",
        "bobot_umum": 0.03, "bobot_bank": 0.00,
        "skala": [(180,None,1),(90,180,2),(30,90,3),(0,30,4),(None,0,5)],
        "inverse": True,
    },
]

# Verifikasi total bobot per stock_type (CCC dikecualikan — conditional)
def _verify_weights():
    def applies(tipe, stype):
        if tipe == "consumer": return False   # skip — conditional
        if tipe == "semua":    return True
        if isinstance(tipe, list): return stype in tipe
        return tipe == stype
    for stype in ["umum", "bank", "karya"]:
        total = sum(
            (c["bobot_bank"] if stype == "bank" else c["bobot_umum"])
            for c in SCORING_CONFIG
            if applies(c["tipe"], stype)
            and (c["bobot_bank"] if stype == "bank" else c["bobot_umum"]) > 0
        )
        # Umum: CCC = 3% untuk Consumer, tapi karena conditional bobot lain normalize
        status = "✅" if abs(total - 1.0) < 0.001 else f"⚠️  diff={total-1.0:+.4f}"
        print(f"  {status}  bobot {stype:5s} = {total:.4f}")

_verify_weights()


# ════════════════════════════════════════════════════════════════
# 6. HELPER FUNCTIONS
# ════════════════════════════════════════════════════════════════

def get_item_safe(df, keywords):
    for k in keywords:
        if k in df.index:
            return df.loc[k]
    return pd.Series([np.nan] * len(df.columns), index=df.columns)

def get_item_ttm(series_data, item_type='flow'):
    try:
        if isinstance(series_data, (int, float, np.number)):
            return series_data
        if len(series_data) == 0 or series_data.isna().all():
            return np.nan
        return series_data.iloc[:4].sum() if item_type == 'flow' else series_data.iloc[0]
    except:
        return np.nan

def get_price_at_date(history_df, target_date):
    try:
        h = history_df.copy()
        if h.index.tz is not None:
            h.index = h.index.tz_localize(None)
        if hasattr(target_date, 'tz_localize'):
            target_date = target_date.tz_localize(None)
        h = h.sort_index()
        idx = h.index.get_indexer([target_date], method='pad')[0]
        return np.nan if idx == -1 else h.iloc[idx]['Close']
    except:
        return np.nan

def safe_div(a, b, default=np.nan):
    try:
        if b == 0 or pd.isna(b) or np.isinf(b):
            return default
        r = a / b
        return r if not (pd.isna(r) or np.isinf(r)) else default
    except:
        return default

GRAHAM_K_DEFAULT = 22.5  # Benjamin Graham original constant — fallback jika sektor tidak dikenali

def get_graham_constants(sector, industry):
    key = (sector, industry)
    if key in GRAHAM_CONSTANTS:
        return GRAHAM_CONSTANTS[key]
    # Fallback level 2: cocok sektor saja (ambil entry pertama)
    for (s, i), v in GRAHAM_CONSTANTS.items():
        if s == sector:
            return v
    # Fallback level 3: tidak ditemukan sama sekali → gunakan K=22.5 (Graham original)
    print(f"      ⚠️  Graham fallback K=22.5 untuk sector='{sector}' industry='{industry}'")
    return {"K": GRAHAM_K_DEFAULT, "target_pe": 15.0, "target_pb": 1.5}

def get_valid_methods(sector):
    if sector == "Financial Services":
        return {"graham": True, "dcf": False, "ev_ebitda": False}
    elif sector == "Technology":
        return {"graham": True, "dcf": True, "ev_ebitda": True}
    elif sector == "Energy":
        return {"graham": True, "dcf": False, "ev_ebitda": True}
    elif sector == "Real Estate":
        return {"graham": True, "dcf": True, "ev_ebitda": False}
    else:
        return {"graham": True, "dcf": True, "ev_ebitda": True}

# ════════════════════════════════════════════════════════════════
# 7. VALUATION FUNCTIONS
# ════════════════════════════════════════════════════════════════

def calc_graham_value(eps, bvps, sector, industry):
    constants = get_graham_constants(sector, industry)
    if constants is None:
        return np.nan, np.nan
    K, target_pb = constants["K"], constants["target_pb"]
    if sector == "Financial Services":
        return (target_pb * bvps, K) if bvps > 0 else (np.nan, K)
    if eps <= 0 or bvps <= 0:
        return np.nan, K
    return np.sqrt(K * eps * bvps), K

def calc_dcf_value(fcf_total_series, shares, beta, debt, cash,
                   revenue_total=np.nan, sector=""):
    """
    DCF berbasis FCF historis.
    Jika FCF positif < 2 tahun (growth/loss-making), fallback ke EV/Sales.
    Returns: (value_per_share, wacc, cagr_or_note)
      cagr_or_note: float jika DCF normal, string "EV/Sales" jika fallback
    """
    wacc = np.clip(RISK_FREE_RATE + beta * EQUITY_RISK_PREM, WACC_MIN, WACC_MAX)

    # ── Cek apakah FCF cukup untuk DCF ───────────────────────────
    has_fcf = False
    if fcf_total_series is not None and len(fcf_total_series) >= 2:
        clean = fcf_total_series.dropna()
        clean = clean[clean > 0]
        if len(clean) >= 2:
            has_fcf = True

    if has_fcf:
        # ── DCF Normal ───────────────────────────────────────────
        n    = len(clean) - 1
        if n <= 0:
            return np.nan, wacc, np.nan
        cagr = np.clip((clean.iloc[-1] / clean.iloc[0]) ** (1/n) - 1, -0.20, wacc * 2)
        base = clean.iloc[-1]
        pv   = sum(base * (1+cagr)**t / (1+wacc)**t for t in range(1, DCF_YEARS+1))
        tv   = base * (1+cagr)**DCF_YEARS * (1+TERMINAL_GROWTH) / (wacc - TERMINAL_GROWTH)
        ev   = pv + tv / (1+wacc)**DCF_YEARS
        val  = safe_div(ev - debt + cash, shares)
        return (val if val > 0 else np.nan), wacc, cagr

    else:
        # ── Fallback: EV/Sales ───────────────────────────────────
        # Pakai ketika FCF tidak cukup positif (startup, growth, loss-making)
        if pd.isna(revenue_total) or revenue_total <= 0 or shares <= 0:
            return np.nan, wacc, np.nan
        mult    = EV_SALES_TARGETS.get(sector, EV_SALES_DEFAULT)
        net_dbt = (debt - cash) if not pd.isna(debt) and not pd.isna(cash) else 0
        ev_val  = mult * revenue_total - net_dbt
        val     = safe_div(ev_val, shares)
        # cagr diganti string marker agar tahu ini EV/Sales
        return (val if val > 0 else np.nan), wacc, "EV/Sales"

def calc_ev_ebitda_value(ebitda, debt, cash, shares, sector):
    if ebitda <= 0 or shares <= 0:
        return np.nan, np.nan
    mult = EV_EBITDA_TARGETS.get(sector, EV_EBITDA_DEFAULT)
    val  = safe_div(mult * ebitda - (debt - cash), shares)
    return (val if val > 0 else np.nan), mult

def calc_weighted_fair_value(g_val, d_val, ev_val):
    vals    = {"graham": g_val, "dcf": d_val, "ev": ev_val}
    weights = {"graham": WEIGHT_GRAHAM, "dcf": WEIGHT_DCF, "ev": WEIGHT_EV_EBITDA}
    valid   = {k: v for k, v in vals.items()
               if v is not None and not (isinstance(v, float) and np.isnan(v))}
    if not valid:
        return np.nan
    total_w = sum(weights[k] for k in valid)
    return sum(v * weights[k] / total_w for k, v in valid.items())

def get_signal(price, fair_value):
    if not fair_value or np.isnan(fair_value) or price <= 0:
        return "N/A"
    up = (fair_value - price) / price
    return "🟢 MURAH" if up >= MOS_MURAH else ("🔴 MAHAL" if up <= MOS_MAHAL else "🟡 WAJAR")

def fmt_cagr(cagr_val):
    """Format CAGR: float → persen, string 'EV/Sales' → tampilkan apa adanya."""
    if cagr_val is None:
        return np.nan
    if isinstance(cagr_val, str):
        return cagr_val          # "EV/Sales"
    if pd.isna(cagr_val):
        return np.nan
    return round(cagr_val * 100, 2)

# ════════════════════════════════════════════════════════════════
# 8. SCORING FUNCTION
# ════════════════════════════════════════════════════════════════

def score_from_skala(value, skala, inverse=False):
    """
    Kembalikan skor 1-5 berdasarkan skala.
    Skala format: list of (lo, hi, skor) — lo=None berarti -inf, hi=None berarti +inf.
    """
    if value is None or pd.isna(value):
        return np.nan
    for lo, hi, s in skala:
        lo_ok = (lo is None) or (value >= lo)
        hi_ok = (hi is None) or (value <  hi)
        if lo_ok and hi_ok:
            return s
    return np.nan

# EV/EBITDA skala per sektor untuk scoring
EV_EBITDA_SCORE_SKALA = {
    "Technology":         [(40,None,1),(30,40,2),(20,30,3),(12,20,4),(None,12,5)],
    "Energy":             [(10,None,1),(8,10,2),(5,8,3),(3,5,4),(None,3,5)],
    "Basic Materials":    [(10,None,1),(8,10,2),(5,8,3),(3,5,4),(None,3,5)],
    # default umum untuk sektor lain
}
EV_EBITDA_SCORE_SKALA_DEFAULT = [(20,None,1),(15,20,2),(10,15,3),(7,10,4),(None,7,5)]

# Sektor consumer untuk CCC
CONSUMER_SECTORS = {"Consumer Defensive", "Consumer Cyclical"}

def calc_fundamental_score(data_row, stock_type, sector=""):
    """
    data_row : dict rasio untuk 1 periode, termasuk kunci baru:
               FCF_to_NI, NetDebt_EBITDA, Revenue_CAGR3, EPS_CAGR3,
               CCC, EV_EBITDA_score_input
    stock_type: "bank" | "karya" | "umum"
    sector    : normalized yfinance sector (untuk CCC & EV/EBITDA skala)
    Returns   : (score_1to10, dict komponen)
    """
    components     = {}
    total_weighted = 0.0
    total_weight   = 0.0
    is_consumer    = sector in CONSUMER_SECTORS

    for cfg in SCORING_CONFIG:
        tipe = cfg["tipe"]

        # ── Cek applicable ────────────────────────────────────
        if tipe == "consumer":
            # CCC: hanya untuk sektor consumer DAN stock_type umum
            applicable = (is_consumer and stock_type == "umum")
        elif tipe == "semua":
            applicable = True
        elif isinstance(tipe, list):
            applicable = stock_type in tipe
        else:
            applicable = (tipe == stock_type)

        if not applicable:
            continue

        bobot = cfg["bobot_bank"] if stock_type == "bank" else cfg["bobot_umum"]
        if bobot == 0:
            continue

        raw_val = data_row.get(cfg["rasio"], np.nan)
        try:
            raw_val = float(raw_val)
        except:
            raw_val = np.nan

        # ── Skala override untuk EV/EBITDA ────────────────────
        if cfg["rasio"] == "EV_EBITDA_score_input":
            skala = EV_EBITDA_SCORE_SKALA.get(sector, EV_EBITDA_SCORE_SKALA_DEFAULT)
        else:
            skala = cfg["skala"]

        # ── Pre-processing khusus ──────────────────────────────
        # FCF/NI: cap atas 3.0 agar tidak bias dari net income sangat kecil
        if cfg["rasio"] == "FCF_to_NI" and not pd.isna(raw_val):
            raw_val = min(raw_val, 3.0)
        # NetDebt/EBITDA: jika negatif (net cash) → skor otomatis 5
        if cfg["rasio"] == "NetDebt_EBITDA" and not pd.isna(raw_val) and raw_val < 0:
            raw_val = -1.0  # masuk bucket (None, 1.0) → skor 5
        # CAGR: konversi ke persen jika masih dalam desimal
        if cfg["rasio"] in ("Revenue_CAGR3", "EPS_CAGR3") and not pd.isna(raw_val):
            if abs(raw_val) < 5:   # kemungkinan masih desimal (0.15 bukan 15)
                raw_val = raw_val * 100

        skor = score_from_skala(raw_val, skala, inverse=cfg.get("inverse", False))

        components[cfg["label"]] = {
            "raw":        raw_val,
            "skor_1to5":  skor,
            "bobot":      bobot,
            "kontribusi": skor * bobot if not pd.isna(skor) else 0.0,
        }
        if not pd.isna(skor):
            total_weighted += skor * bobot
            total_weight   += bobot

    # Normalize & skala ke 1-10
    normalized_5 = safe_div(total_weighted, total_weight, default=np.nan)
    score_1to10  = round(normalized_5 * 2, 2) if not pd.isna(normalized_5) else np.nan
    return score_1to10, components

def score_label(score):
    if pd.isna(score):     return "N/A"
    if score >= 8.0:       return "⭐ SANGAT BAGUS"
    if score >= 6.5:       return "✅ BAGUS"
    if score >= 5.0:       return "🟡 CUKUP"
    if score >= 3.5:       return "⚠️ KURANG"
    return "❌ BURUK"

# ════════════════════════════════════════════════════════════════
# 9. MAIN LOOP
# ════════════════════════════════════════════════════════════════
print(f"\n🚀 Memproses {len(DAFTAR_SAHAM)} saham...\n")
all_valuation = []
all_details   = []

for ticker in DAFTAR_SAHAM:
    print(f"🔹 {ticker}...", end=" ")
    try:
        stock      = yf.Ticker(ticker)
        info       = stock.info
        raw_sector = info.get('sector', 'Unknown')
        raw_indust = info.get('industry', 'Unknown')

        sector   = normalize_sector(raw_sector)
        industry = normalize_industry(raw_indust)

        beta_raw = info.get('beta', 1.0) or 1.0
        beta     = float(np.clip(beta_raw, 0.3, 3.0))
        stock_type = get_stock_type(sector, industry)

        # Ambil semua data finansial
        is_ann = stock.financials
        bs_ann = stock.balance_sheet
        cf_ann = stock.cashflow
        is_q   = stock.quarterly_financials
        bs_q   = stock.quarterly_balance_sheet
        cf_q   = stock.quarterly_cashflow

        if is_ann.empty:
            print("❌ Tidak ada data."); continue

        hist = stock.history(period="10y")
        if hist.empty:
            print("❌ Tidak ada history harga."); continue

        shares_now = info.get('sharesOutstanding',
                     info.get('impliedSharesOutstanding', np.nan))

        # Helper lambdas
        def ann(k):  return get_item_safe(is_ann, k)
        def bsa(k):  return get_item_safe(bs_ann, k)
        def cfa(k):  return get_item_safe(cf_ann, k)

        net_income_a = ann(["Net Income", "NetIncome", "Net Income Common Stockholders"])
        revenue_a    = ann(["Total Revenue", "TotalRevenue", "Operating Revenue"])
        op_income_a  = ann(["Operating Income", "OperatingIncome", "EBIT"])
        gross_p_a    = ann(["Gross Profit", "GrossProfit"])
        int_exp_a    = ann(["Interest Expense", "InterestExpense"]).abs()
        da_a         = cfa(["Depreciation & Amortization",
                             "DepreciationAndAmortization", "Reconciled Depreciation"])
        ocf_a        = cfa(["Total Cash From Operating Activities", "Operating Cash Flow",
                             "Cash Flow From Continuing Operating Activities"])
        capex_a      = cfa(["Capital Expenditure", "Capital Expenditures"]).fillna(0)
        equity_a     = bsa(["Stockholders Equity", "StockholdersEquity",
                             "Total Equity Gross Minority Interest"])
        debt_a       = bsa(["Total Debt", "Long Term Debt And Capital Lease Obligation",
                             "Long Term Debt"])
        cash_a       = bsa(["Cash And Cash Equivalents",
                             "Cash Cash Equivalents And Short Term Investments"])
        curr_ass_a   = bsa(["Current Assets", "CurrentAssets"])
        curr_lia_a   = bsa(["Current Liabilities", "CurrentLiabilities"])
        inv_a        = bsa(["Inventory"])
        rec_a        = bsa(["Accounts Receivable", "Net Receivables", "Receivables"])
        tot_ass_a    = bsa(["Total Assets", "TotalAssets"])
        tot_lia_a    = bsa(["Total Liabilities Net Minority Interest", "Total Liabilities"])
        shares_a     = bsa(["Share Issued", "Ordinary Shares Number", "Common Stock"])
        shares_a     = shares_a.replace([0, np.nan], shares_now)
        div_a        = cfa(["Cash Dividends Paid", "CashDividendsPaid"]).abs()

        fcf_a = (ocf_a + capex_a)
        fcf_a.index = [d.year for d in fcf_a.index]
        fcf_a = fcf_a.sort_index()

        ebitda_a = op_income_a + da_a.fillna(0)
        valid_methods = get_valid_methods(sector)

        # ── PER TAHUN ────────────────────────────────────────────
        for col_date in list(is_ann.columns):
            yr = col_date.year
            try:
                price = get_price_at_date(hist, col_date)
                if pd.isna(price) or price <= 0: continue

                sh      = shares_a.get(col_date, shares_now) or shares_now
                rev     = revenue_a.get(col_date, np.nan)    or 0
                gp      = gross_p_a.get(col_date, np.nan)    or 0
                op      = op_income_a.get(col_date, np.nan)  or 0
                ni      = net_income_a.get(col_date, np.nan) or 0
                assets  = tot_ass_a.get(col_date, np.nan)    or 0
                eq      = equity_a.get(col_date, np.nan)     or 0
                lia     = tot_lia_a.get(col_date, np.nan)    or 0
                debt    = debt_a.get(col_date, np.nan)        or 0
                cash_v  = cash_a.get(col_date, np.nan)        or 0
                curr_a  = curr_ass_a.get(col_date, np.nan)   or 0
                curr_l  = curr_lia_a.get(col_date, np.nan)   or 0
                inv_v   = inv_a.get(col_date, np.nan)         or 0
                rec_v   = rec_a.get(col_date, np.nan)         or 0
                int_e   = int_exp_a.get(col_date, np.nan)    or 0
                da_v    = da_a.get(col_date, np.nan)          or 0
                div_v   = div_a.get(col_date, np.nan)         or 0
                ebitda  = ebitda_a.get(col_date, np.nan)      or 0

                eps  = safe_div(ni, sh, 0)
                bvps = safe_div(eq, sh, 0)

                # Rasio dasar
                gpm     = safe_div(gp,  rev,    0) * 100
                opm     = safe_div(op,  rev,    0) * 100
                npm     = safe_div(ni,  rev,    0) * 100
                roe     = safe_div(ni,  eq,     0) * 100
                roa     = safe_div(ni,  assets, 0) * 100
                der     = safe_div(lia, eq,     0)
                dar     = safe_div(lia, assets, 0)
                cr      = safe_div(curr_a, curr_l, 0)
                cogs_v  = (rev - gp) if gp else rev
                inv_to  = safe_div(cogs_v, inv_v, 0) if inv_v else np.nan
                rec_to  = safe_div(rev, rec_v, 0) if rec_v else np.nan
                at      = safe_div(rev, assets, 0)
                int_cov = safe_div(op, int_e, 0) if int_e else np.nan
                dps     = safe_div(div_v, sh, 0)
                div_yld = safe_div(dps, price, 0) * 100
                per     = safe_div(price, eps, 0) if eps > 0 else np.nan
                pbv     = safe_div(price, bvps, 0) if bvps > 0 else np.nan

                # ── Komponen baru ──────────────────────────────
                # FCF / Net Income (kualitas laba)
                ocf_v   = ocf_a.get(col_date, np.nan) or 0
                cpx_v   = capex_a.get(col_date, 0) or 0
                fcf_v   = ocf_v + cpx_v
                fcf_ni  = safe_div(fcf_v, ni, np.nan) if ni and ni > 0 else np.nan

                # Net Debt / EBITDA
                net_debt   = debt - cash_v
                nd_ebitda  = safe_div(net_debt, ebitda, np.nan) if ebitda and ebitda > 0 else np.nan

                # Revenue CAGR 3Y (%)
                rev_series = revenue_a.dropna()
                rev_series.index = [d.year for d in rev_series.index]
                rev_series = rev_series[rev_series > 0].sort_index()
                rev_base   = rev_series.get(yr - 3, np.nan)
                rev_cagr   = (safe_div(rev, rev_base) ** (1/3) - 1) * 100 if rev > 0 and not pd.isna(rev_base) and rev_base > 0 else np.nan

                # EPS CAGR 3Y (%)
                ni_series  = net_income_a.dropna()
                ni_series.index  = [d.year for d in ni_series.index]
                sh_series  = shares_a.dropna()
                sh_series.index  = [d.year for d in sh_series.index]
                eps_base_ni = ni_series.get(yr - 3, np.nan)
                eps_base_sh = sh_series.get(yr - 3, shares_now)
                eps_base   = safe_div(eps_base_ni, eps_base_sh, np.nan)
                eps_cagr   = (safe_div(eps, eps_base) ** (1/3) - 1) * 100 if eps > 0 and not pd.isna(eps_base) and eps_base > 0 else np.nan

                # CCC — Days Inventory + Days Receivable - Days Payable
                pay_a_col  = bs_ann
                payables_v = get_item_safe(pay_a_col, ["Accounts Payable","AccountsPayable","Payables"]).get(col_date, 0) or 0
                dio = safe_div(inv_v, cogs_v, np.nan) * 365 if inv_v and cogs_v else np.nan
                dso = safe_div(rec_v, rev, np.nan)    * 365 if rec_v and rev else np.nan
                dpo = safe_div(payables_v, cogs_v, np.nan) * 365 if payables_v and cogs_v else np.nan
                ccc = (dio + dso - dpo) if not any(pd.isna(x) for x in [dio, dso, dpo]) else np.nan

                # EV/EBITDA actual (recycle dari valuation)
                mkt_cap        = price * sh if price and sh else np.nan
                ev_actual      = (mkt_cap + debt - cash_v) if not pd.isna(mkt_cap) else np.nan
                ev_ebitda_act  = safe_div(ev_actual, ebitda, np.nan) if ebitda and ebitda > 0 else np.nan

                data_row = {
                    "GPM %":                 gpm,
                    "OPM %":                 opm,
                    "NPM %":                 npm,
                    "ROE %":                 roe,
                    "ROA %":                 roa,
                    "DER (x)":               der,
                    "DAR (x)":               dar,
                    "Interest_Coverage":     int_cov,
                    "Risk Factor":           np.nan,
                    "Asset Turnover (x)":    at,
                    "PER (x)":               per,
                    "PBV (x)":               pbv,
                    "Dividend Yield %":      div_yld,
                    "Current Ratio (x)":     cr,
                    "Inventory Turnover":    inv_to,
                    "Receivables TO":        rec_to,
                    # ── Baru ──
                    "FCF_to_NI":             fcf_ni,
                    "NetDebt_EBITDA":        nd_ebitda,
                    "Revenue_CAGR3":         rev_cagr,
                    "EPS_CAGR3":             eps_cagr,
                    "CCC":                   ccc,
                    "EV_EBITDA_score_input": ev_ebitda_act,
                }

                score, comp = calc_fundamental_score(data_row, stock_type, sector=sector)

                # Valuasi
                fcf_to_yr = fcf_a[fcf_a.index <= yr]
                g_val, g_k = calc_graham_value(eps, bvps, sector, industry) if valid_methods["graham"] else (np.nan, np.nan)
                d_val, d_wacc, d_cagr = calc_dcf_value(fcf_to_yr, sh, beta, debt, cash_v, revenue_total=rev, sector=sector) if valid_methods["dcf"] else (np.nan, np.nan, np.nan)
                ev_val, ev_mult = calc_ev_ebitda_value(ebitda, debt, cash_v, sh, sector) if valid_methods["ev_ebitda"] else (np.nan, np.nan)

                g_sig  = get_signal(price, g_val)  if not pd.isna(g_val)  else "N/A"
                d_sig  = get_signal(price, d_val)  if not pd.isna(d_val)  else "N/A"
                ev_sig = get_signal(price, ev_val) if not pd.isna(ev_val) else "N/A"
                # Label: DCF atau EV/Sales tergantung metode yang dipakai
                dcf_method_label = "EV/Sales" if isinstance(d_cagr, str) else "DCF"

                fair   = calc_weighted_fair_value(g_val, d_val, ev_val)
                upside = safe_div(fair - price, price) * 100 if not pd.isna(fair) else np.nan
                mos    = safe_div(fair - price, fair)  * 100 if not pd.isna(fair) and fair > 0 else np.nan

                # ── Append VALUATION ──
                all_valuation.append({
                    "Ticker":               ticker,
                    "Sector (Yahoo)":       sector,
                    "Industry (Yahoo)":     industry,
                    "Stock_Type":           stock_type,
                    "Year":                 yr,
                    "Price":                round(price, 2),
                    "Beta":                 round(beta, 2),
                    "WACC (%)":             round(d_wacc * 100, 2) if not pd.isna(d_wacc) else np.nan,
                    "Graham_K":             round(g_k, 4) if not pd.isna(g_k) else np.nan,
                    "Graham_Value":         round(g_val, 2) if not pd.isna(g_val) else np.nan,
                    "Graham_Signal":        g_sig,
                    "DCF_Method":           dcf_method_label,
                    "DCF_FCF_CAGR (%)":     fmt_cagr(d_cagr) if dcf_method_label == "DCF" else np.nan,
                    "EV_Sales_Multiple":    EV_SALES_TARGETS.get(sector, EV_SALES_DEFAULT) if dcf_method_label == "EV/Sales" else np.nan,
                    "DCF_Value":            round(d_val, 2) if not pd.isna(d_val) else np.nan,
                    "DCF_Signal":           d_sig,
                    "EV_EBITDA_Multiple":   round(ev_mult, 1) if not pd.isna(ev_mult) else np.nan,
                    "EV_EBITDA_Value":      round(ev_val, 2) if not pd.isna(ev_val) else np.nan,
                    "EV_EBITDA_Signal":     ev_sig,
                    "Fair_Value":           round(fair, 2) if not pd.isna(fair) else np.nan,
                    "Upside_Pct (%)":       round(upside, 2) if not pd.isna(upside) else np.nan,
                    "Margin_of_Safety (%)": round(mos, 2) if not pd.isna(mos) else np.nan,
                    "Final_Signal":         get_signal(price, fair),
                    "Funda_Score":          round(score, 2) if not pd.isna(score) else np.nan,
                    "Funda_Label":          score_label(score),
                })

                # ── Append DETAILS ──
                detail_base = {
                    "Ticker": ticker, "Sector": sector, "Industry": industry,
                    "Stock_Type": stock_type, "Year": yr, "Price": round(price, 2),
                    "Funda_Score": round(score, 2) if not pd.isna(score) else np.nan,
                    "Funda_Label": score_label(score),
                }
                for lbl, c in comp.items():
                    raw = c["raw"]
                    detail_base[f"{lbl} | Raw"]       = round(raw, 4) if not pd.isna(raw) else np.nan
                    detail_base[f"{lbl} | Skor(1-5)"] = c["skor_1to5"]
                    detail_base[f"{lbl} | Bobot"]     = c["bobot"]
                    detail_base[f"{lbl} | Kontribusi"] = round(c["kontribusi"], 4)
                all_details.append(detail_base)

            except Exception as e_inner:
                print(f"  [skip {yr}: {e_inner}]", end=" ")

        # ── TTM ──────────────────────────────────────────────────
        if not is_q.empty and not bs_q.empty:
            try:
                def qttm(k, t='flow'): return get_item_ttm(get_item_safe(is_q, k), t)
                def bttm(k):           return get_item_ttm(get_item_safe(bs_q, k), 'snapshot')
                def cfttm(k, t='flow'):return get_item_ttm(get_item_safe(cf_q, k), t)

                ni_t    = qttm(["Net Income", "NetIncome"]) or 0
                rev_t   = qttm(["Total Revenue", "TotalRevenue"]) or 0
                gp_t    = qttm(["Gross Profit"]) or 0
                op_t    = qttm(["Operating Income", "EBIT"]) or 0
                ocf_t   = cfttm(["Total Cash From Operating Activities", "Operating Cash Flow"]) or 0
                capx_t  = cfttm(["Capital Expenditure", "Capital Expenditures"]) or 0
                da_t    = cfttm(["Depreciation & Amortization", "DepreciationAndAmortization",
                                  "Reconciled Depreciation"]) or 0
                div_t   = cfttm(["Cash Dividends Paid", "CashDividendsPaid"]) or 0
                int_t   = abs(qttm(["Interest Expense", "InterestExpense"]) or 0)

                eq_t    = bttm(["Stockholders Equity", "StockholdersEquity"]) or 0
                debt_t  = bttm(["Total Debt", "Long Term Debt And Capital Lease Obligation"]) or 0
                cash_t  = bttm(["Cash And Cash Equivalents",
                                  "Cash Cash Equivalents And Short Term Investments"]) or 0
                ca_t    = bttm(["Current Assets", "CurrentAssets"]) or 0
                cl_t    = bttm(["Current Liabilities", "CurrentLiabilities"]) or 0
                inv_t   = bttm(["Inventory"]) or 0
                rec_t   = bttm(["Accounts Receivable", "Net Receivables"]) or 0
                ass_t   = bttm(["Total Assets", "TotalAssets"]) or 0
                lia_t   = bttm(["Total Liabilities Net Minority Interest",
                                  "Total Liabilities"]) or 0

                sh_t    = shares_now or 1
                price_t = hist.iloc[-1]['Close']

                # Risk Factor TTM (volatilitas 1 tahun)
                h2 = hist.copy()
                if h2.index.tz is not None: h2.index = h2.index.tz_localize(None)
                recent = h2[h2.index >= h2.index[-1] - pd.Timedelta(days=365)]['Close']
                rf_ttm = recent.pct_change().std() * np.sqrt(252) * 100 if len(recent) > 50 else np.nan

                eps_t   = safe_div(ni_t,  sh_t, 0)
                bvps_t  = safe_div(eq_t,  sh_t, 0)
                dps_t   = safe_div(abs(div_t), sh_t, 0)
                ebitda_t= op_t + da_t
                cogs_t  = (rev_t - gp_t) if gp_t else rev_t

                gpm_t    = safe_div(gp_t,  rev_t,  0) * 100
                opm_t    = safe_div(op_t,  rev_t,  0) * 100
                npm_t    = safe_div(ni_t,  rev_t,  0) * 100
                roe_t    = safe_div(ni_t,  eq_t,   0) * 100
                roa_t    = safe_div(ni_t,  ass_t,  0) * 100
                der_t    = safe_div(lia_t, eq_t,   0)
                dar_t    = safe_div(lia_t, ass_t,  0)
                cr_t     = safe_div(ca_t,  cl_t,   0)
                inv_to_t = safe_div(cogs_t, inv_t,  np.nan) if inv_t  else np.nan
                rec_to_t = safe_div(rev_t,  rec_t,  np.nan) if rec_t  else np.nan
                at_t     = safe_div(rev_t,  ass_t,  0)
                intcov_t = safe_div(op_t,   int_t,  np.nan) if int_t  else np.nan
                dyld_t   = safe_div(dps_t,  price_t, 0) * 100
                per_t    = safe_div(price_t, eps_t,  np.nan) if eps_t  > 0 else np.nan
                pbv_t    = safe_div(price_t, bvps_t, np.nan) if bvps_t > 0 else np.nan

                # ── Komponen baru TTM ──────────────────────────
                # FCF / Net Income
                fcf_ttm_v = ocf_t + capx_t
                fcf_ni_t  = safe_div(fcf_ttm_v, ni_t, np.nan) if ni_t and ni_t > 0 else np.nan

                # Net Debt / EBITDA
                net_debt_t  = debt_t - cash_t
                nd_ebitda_t = safe_div(net_debt_t, ebitda_t, np.nan) if ebitda_t and ebitda_t > 0 else np.nan

                # Revenue CAGR 3Y — bandingkan TTM vs revenue 3 tahun lalu
                rev_series_t = revenue_a.dropna()
                rev_series_t.index = [d.year for d in rev_series_t.index]
                rev_series_t = rev_series_t[rev_series_t > 0].sort_index()
                cur_yr   = datetime.now().year
                rev_3ago = rev_series_t.get(cur_yr - 3,
                           rev_series_t.get(cur_yr - 4, np.nan))
                rev_cagr_t = (safe_div(rev_t, rev_3ago) ** (1/3) - 1) * 100                              if rev_t > 0 and not pd.isna(rev_3ago) and rev_3ago > 0 else np.nan

                # EPS CAGR 3Y
                ni_ser_t  = net_income_a.dropna()
                ni_ser_t.index  = [d.year for d in ni_ser_t.index]
                sh_ser_t  = shares_a.dropna()
                sh_ser_t.index  = [d.year for d in sh_ser_t.index]
                eps_3ago_ni = ni_ser_t.get(cur_yr - 3, ni_ser_t.get(cur_yr - 4, np.nan))
                eps_3ago_sh = sh_ser_t.get(cur_yr - 3, sh_ser_t.get(cur_yr - 4, sh_t))
                eps_3ago    = safe_div(eps_3ago_ni, eps_3ago_sh, np.nan)
                eps_cagr_t  = (safe_div(eps_t, eps_3ago) ** (1/3) - 1) * 100                               if eps_t > 0 and not pd.isna(eps_3ago) and eps_3ago > 0 else np.nan

                # CCC TTM
                pay_t  = bttm(["Accounts Payable", "AccountsPayable", "Payables"]) or 0
                dio_t  = safe_div(inv_t,  cogs_t,  np.nan) * 365 if inv_t  and cogs_t  else np.nan
                dso_t  = safe_div(rec_t,  rev_t,   np.nan) * 365 if rec_t  and rev_t   else np.nan
                dpo_t  = safe_div(pay_t,  cogs_t,  np.nan) * 365 if pay_t  and cogs_t  else np.nan
                ccc_t  = (dio_t + dso_t - dpo_t)                          if not any(pd.isna(x) for x in [dio_t, dso_t, dpo_t]) else np.nan

                # EV/EBITDA TTM (recycle)
                mkt_cap_t     = price_t * sh_t if price_t and sh_t else np.nan
                ev_actual_t   = (mkt_cap_t + debt_t - cash_t) if not pd.isna(mkt_cap_t) else np.nan
                ev_ebitda_t_s = safe_div(ev_actual_t, ebitda_t, np.nan) if ebitda_t and ebitda_t > 0 else np.nan

                data_row_t = {
                    "GPM %":                 gpm_t,
                    "OPM %":                 opm_t,
                    "NPM %":                 npm_t,
                    "ROE %":                 roe_t,
                    "ROA %":                 roa_t,
                    "DER (x)":               der_t,
                    "DAR (x)":               dar_t,
                    "Interest_Coverage":     intcov_t,
                    "Risk Factor":           rf_ttm,
                    "Asset Turnover (x)":    at_t,
                    "PER (x)":               per_t,
                    "PBV (x)":               pbv_t,
                    "Dividend Yield %":      dyld_t,
                    "Current Ratio (x)":     cr_t,
                    "Inventory Turnover":    inv_to_t,
                    "Receivables TO":        rec_to_t,
                    # ── Baru ──
                    "FCF_to_NI":             fcf_ni_t,
                    "NetDebt_EBITDA":        nd_ebitda_t,
                    "Revenue_CAGR3":         rev_cagr_t,
                    "EPS_CAGR3":             eps_cagr_t,
                    "CCC":                   ccc_t,
                    "EV_EBITDA_score_input": ev_ebitda_t_s,
                }

                score_t, comp_t = calc_fundamental_score(data_row_t, stock_type, sector=sector)

                # FCF TTM series
                fcf_all_t = fcf_a.copy()
                fcf_ttm_val = ocf_t + capx_t
                if fcf_ttm_val > 0:
                    fcf_all_t[datetime.now().year] = fcf_ttm_val

                g_val_t, g_k_t = calc_graham_value(eps_t, bvps_t, sector, industry) if valid_methods["graham"] else (np.nan, np.nan)
                d_val_t, d_w_t, d_c_t = calc_dcf_value(fcf_all_t, sh_t, beta, debt_t, cash_t, revenue_total=rev_t, sector=sector) if valid_methods["dcf"] else (np.nan, np.nan, np.nan)
                ev_val_t, ev_m_t = calc_ev_ebitda_value(ebitda_t, debt_t, cash_t, sh_t, sector) if valid_methods["ev_ebitda"] else (np.nan, np.nan)

                fair_t  = calc_weighted_fair_value(g_val_t, d_val_t, ev_val_t)
                up_t    = safe_div(fair_t - price_t, price_t) * 100 if not pd.isna(fair_t) else np.nan
                mos_t   = safe_div(fair_t - price_t, fair_t)  * 100 if not pd.isna(fair_t) and fair_t > 0 else np.nan
                dcf_method_label_t = "EV/Sales" if isinstance(d_c_t, str) else "DCF"

                all_valuation.append({
                    "Ticker":               ticker,
                    "Sector (Yahoo)":       sector,
                    "Industry (Yahoo)":     industry,
                    "Stock_Type":           stock_type,
                    "Year":                 "TTM",
                    "Price":                round(price_t, 2),
                    "Beta":                 round(beta, 2),
                    "WACC (%)":             round(d_w_t * 100, 2) if not pd.isna(d_w_t) else np.nan,
                    "Graham_K":             round(g_k_t, 4) if not pd.isna(g_k_t) else np.nan,
                    "Graham_Value":         round(g_val_t, 2) if not pd.isna(g_val_t) else np.nan,
                    "Graham_Signal":        get_signal(price_t, g_val_t),
                    "DCF_Method":           dcf_method_label_t,
                    "DCF_FCF_CAGR (%)":     fmt_cagr(d_c_t) if dcf_method_label_t == "DCF" else np.nan,
                    "EV_Sales_Multiple":    EV_SALES_TARGETS.get(sector, EV_SALES_DEFAULT) if dcf_method_label_t == "EV/Sales" else np.nan,
                    "DCF_Value":            round(d_val_t, 2) if not pd.isna(d_val_t) else np.nan,
                    "DCF_Signal":           get_signal(price_t, d_val_t),
                    "EV_EBITDA_Multiple":   round(ev_m_t, 1) if not pd.isna(ev_m_t) else np.nan,
                    "EV_EBITDA_Value":      round(ev_val_t, 2) if not pd.isna(ev_val_t) else np.nan,
                    "EV_EBITDA_Signal":     get_signal(price_t, ev_val_t),
                    "Fair_Value":           round(fair_t, 2) if not pd.isna(fair_t) else np.nan,
                    "Upside_Pct (%)":       round(up_t, 2) if not pd.isna(up_t) else np.nan,
                    "Margin_of_Safety (%)": round(mos_t, 2) if not pd.isna(mos_t) else np.nan,
                    "Final_Signal":         get_signal(price_t, fair_t),
                    "Funda_Score":          round(score_t, 2) if not pd.isna(score_t) else np.nan,
                    "Funda_Label":          score_label(score_t),
                })

                detail_ttm = {
                    "Ticker": ticker, "Sector": sector, "Industry": industry,
                    "Stock_Type": stock_type, "Year": "TTM", "Price": round(price_t, 2),
                    "Funda_Score": round(score_t, 2) if not pd.isna(score_t) else np.nan,
                    "Funda_Label": score_label(score_t),
                }
                for lbl, c in comp_t.items():
                    raw = c["raw"]
                    detail_ttm[f"{lbl} | Raw"]        = round(raw, 4) if not pd.isna(raw) else np.nan
                    detail_ttm[f"{lbl} | Skor(1-5)"]  = c["skor_1to5"]
                    detail_ttm[f"{lbl} | Bobot"]      = c["bobot"]
                    detail_ttm[f"{lbl} | Kontribusi"]  = round(c["kontribusi"], 4)
                all_details.append(detail_ttm)

            except Exception as e_ttm:
                print(f"  [TTM err: {e_ttm}]", end=" ")

        print("✅")

    except Exception as e:
        print(f"❌ Error: {e}")


# ════════════════════════════════════════════════════════════════
# 10. BACKTEST FUNCTIONS
# ════════════════════════════════════════════════════════════════

def get_trading_day(hist_df, target_date):
    """
    Ambil hari trading terdekat >= target_date.
    hist_df index harus sudah tz-naive.
    """
    h = hist_df.copy()
    if h.index.tz is not None:
        h.index = h.index.tz_localize(None)
    h = h.sort_index()
    future = h[h.index >= target_date]
    if future.empty:
        return None, None
    return future.index[0], future.iloc[0]


def calc_vwap_first_week(hist_df, start_date):
    """
    VWAP = rata-rata Close × Volume selama 5 hari trading pertama mulai start_date.
    Jika Volume tidak tersedia, pakai rata-rata Close saja.
    """
    h = hist_df.copy()
    if h.index.tz is not None:
        h.index = h.index.tz_localize(None)
    h = h.sort_index()
    week = h[h.index >= start_date].head(5)
    if week.empty:
        return np.nan
    if 'Volume' in week.columns and week['Volume'].sum() > 0:
        vwap = (week['Close'] * week['Volume']).sum() / week['Volume'].sum()
    else:
        vwap = week['Close'].mean()
    return round(vwap, 2)


def run_backtest(df_val, hist_cache):
    """
    Enrichment backtest untuk setiap baris df_val.
    - Skip jika Year == 'TTM'
    - Window: 1 Apr (T+1) → 31 Des (T+1)
    - Entry: VWAP 5 hari trading pertama April T+1
    - High/Low: dari harga Close dalam window
    - Close: harga Close hari trading terakhir Desember T+1
    - Return cols: to_high, to_close, to_yearend (same as to_close window ini),
                   max_drawdown dari entry ke low

    hist_cache: dict {ticker: history_df} sudah diambil sebelumnya
    """
    BT_COLS = [
        'BT_Entry_Price', 'BT_Entry_Date',
        'BT_High_Price',  'BT_High_Date',
        'BT_Low_Price',   'BT_Low_Date',
        'BT_Close_Price', 'BT_Close_Date',
        'BT_Return_to_High (%)',
        'BT_Return_to_Close (%)',
        'BT_Max_Drawdown (%)',
        'BT_Data_Status',
    ]

    results = []
    today = pd.Timestamp.today().normalize()

    for _, row in df_val.iterrows():
        rec = {c: np.nan for c in BT_COLS}
        rec['BT_Data_Status'] = 'N/A'

        year = row['Year']

        # Skip TTM
        if str(year) == 'TTM':
            rec['BT_Data_Status'] = 'SKIP_TTM'
            results.append(rec)
            continue

        ticker = row['Ticker']
        try:
            yr = int(year)
        except:
            results.append(rec)
            continue

        # Window: 1 Apr T+1 → 31 Des T+1
        window_start = pd.Timestamp(yr + 1, 4,  1)
        window_end   = pd.Timestamp(yr + 1, 12, 31)

        # Belum terjadi
        if window_start > today:
            rec['BT_Data_Status'] = 'FUTURE'
            results.append(rec)
            continue

        # Ambil history dari cache
        hist = hist_cache.get(ticker)
        if hist is None or hist.empty:
            rec['BT_Data_Status'] = 'NO_DATA'
            results.append(rec)
            continue

        h = hist.copy()
        if h.index.tz is not None:
            h.index = h.index.tz_localize(None)
        h = h.sort_index()

        # Slice window
        window_data = h[(h.index >= window_start) & (h.index <= window_end)]

        if window_data.empty:
            rec['BT_Data_Status'] = 'NO_WINDOW_DATA'
            results.append(rec)
            continue

        # Partial window (window_end belum terjadi = masih dalam tahun berjalan)
        is_partial = window_end > today

        # ── Entry: VWAP 5 hari trading pertama April ──
        entry_price = calc_vwap_first_week(h, window_start)
        first_day, _ = get_trading_day(h, window_start)
        rec['BT_Entry_Price'] = entry_price
        rec['BT_Entry_Date']  = first_day.strftime('%Y-%m-%d') if first_day is not None else np.nan

        if pd.isna(entry_price) or entry_price <= 0:
            rec['BT_Data_Status'] = 'NO_ENTRY'
            results.append(rec)
            continue

        # ── High dalam window ──
        high_idx   = window_data['Close'].idxmax()
        high_price = window_data['Close'].max()
        rec['BT_High_Price'] = round(high_price, 2)
        rec['BT_High_Date']  = high_idx.strftime('%Y-%m-%d')

        # ── Low dalam window ──
        low_idx   = window_data['Close'].idxmin()
        low_price = window_data['Close'].min()
        rec['BT_Low_Price'] = round(low_price, 2)
        rec['BT_Low_Date']  = low_idx.strftime('%Y-%m-%d')

        # ── Close: hari trading terakhir Desember T+1 ──
        dec_data = window_data[window_data.index.month == 12]
        if not dec_data.empty:
            close_price = dec_data['Close'].iloc[-1]
            close_date  = dec_data.index[-1]
        else:
            # Fallback: hari terakhir window yang tersedia
            close_price = window_data['Close'].iloc[-1]
            close_date  = window_data.index[-1]

        rec['BT_Close_Price'] = round(close_price, 2)
        rec['BT_Close_Date']  = close_date.strftime('%Y-%m-%d')

        # ── Returns ──
        def pct(end, start):
            if pd.isna(start) or start == 0: return np.nan
            return round((end - start) / start * 100, 2)

        rec['BT_Return_to_High (%)']  = pct(high_price,  entry_price)
        rec['BT_Return_to_Close (%)'] = pct(close_price, entry_price)
        rec['BT_Max_Drawdown (%)']    = pct(low_price,   entry_price)  # negatif = drawdown

        rec['BT_Data_Status'] = 'PARTIAL' if is_partial else 'COMPLETE'
        results.append(rec)

    return pd.DataFrame(results, index=df_val.index)

# ════════════════════════════════════════════════════════════════
# 11. EXPORT KE GOOGLE SHEETS
# ════════════════════════════════════════════════════════════════
def sort_and_clean(df):
    df['_sy'] = df['Year'].apply(lambda x: 9999 if str(x) == 'TTM' else int(x))
    df = df.sort_values(['Ticker', '_sy'], ascending=[True, False]).drop(columns=['_sy'])
    df = df.replace([np.inf, -np.inf], np.nan)
    return df.reset_index(drop=True)

def upload_sheet(sh, df, name, rows=750, cols=50):
    try:
        ws = sh.worksheet(name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=name, rows=rows, cols=cols)
    ws.clear()
    set_with_dataframe(ws, df, include_index=False)
    ws.freeze(rows=1)
    print(f"   ✅ Sheet '{name}': {len(df)} baris, {len(df.columns)} kolom")

if all_valuation:
    df_val = sort_and_clean(pd.DataFrame(all_valuation))
    df_det = sort_and_clean(pd.DataFrame(all_details))

    # ── Backtest enrichment ──────────────────────────────────────
    print("\n📈 Menghitung backtest (ambil history harga per ticker)...")
    hist_cache = {}
    unique_tickers = df_val['Ticker'].unique()
    for i, t in enumerate(unique_tickers):
        try:
            h = yf.Ticker(t).history(period="15y")
            if not h.empty:
                hist_cache[t] = h
            print(f"   [{i+1}/{len(unique_tickers)}] {t} — {len(h)} baris", end="\r")
        except Exception as e:
            print(f"   ⚠️  {t}: {e}")

    print(f"\n   ✅ History cache: {len(hist_cache)} tickers")

    df_bt = run_backtest(df_val, hist_cache)
    df_val = pd.concat([df_val, df_bt], axis=1)

    # Status summary
    status_counts = df_bt['BT_Data_Status'].value_counts()
    print(f"   Backtest status: {dict(status_counts)}")

    print(f"\n📤 Uploading ke Google Sheets...")
    sh = gc.open_by_key(SPREADSHEET_ID)
    upload_sheet(sh, df_val, SHEET_VALUATION, cols=60)
    upload_sheet(sh, df_det, SHEET_DETAILS)

    print(f"\n🎉 SUKSES!")
    print(f"   📊 {len(DAFTAR_SAHAM)} saham | {len(df_val)} baris valuation")
    print(f"   🔗 https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")
else:
    print("❌ Tidak ada data yang berhasil diproses.")
