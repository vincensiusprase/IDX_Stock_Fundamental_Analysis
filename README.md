# 📊 IDX End-to-End Valuation & Automated Analytics System

Sistem otomatisasi pipeline data untuk melakukan analisis fundamental, valuasi multi-metode, dan konsolidasi data emiten di seluruh sektor Bursa Efek Indonesia (IDX).

## 📌 Overview
Proyek ini bukan sekadar alat hitung, melainkan sebuah **Data Pipeline** terintegrasi yang menghubungkan ekstraksi data mentah dengan dashboard analitik otomatis. Sistem ini mencakup seluruh sektor (Techno, Energy, Finance, dll.) dan mengelola ratusan emiten secara simultan.

### Alur Kerja Sistem (Workflow)
1. **Python Engine:** Mengekstraksi data laporan keuangan (TTM & Tahunan) via Yahoo Finance API, menghitung skor fundamental, dan melakukan valuasi.
2. **Google Sheets Storage:** Mengunggah hasil kalkulasi ke sheet spesifik berdasarkan sektor emiten (misal: `IDXTECHNO_VALUATION`, `IDXENERGY_VALUATION`).
3. **Apps Script Compiler:** Skrip otomatis di dalam Google Sheets yang menggabungkan seluruh data sektoral menjadi satu laporan konsolidasi (`COMPILE_VALUATION` & `COMPILE_DETAILS`).

## 🚀 Fitur Utama

### 1. Advanced Valuation Engine (Python)
Menghitung *Fair Value* menggunakan bobot campuran dari tiga metodologi:
* **Discounted Cash Flow (DCF):** Berbasis *Free Cash Flow* dengan fallback *EV/Sales* untuk perusahaan *growth*[cite: 1].
* **Adjusted Graham Number:** Modifikasi rumus Benjamin Graham dengan konstanta ($K$) dinamis per industri[cite: 1].
* **Relative Valuation:** Target *EV/EBITDA* spesifik sub-sektor[cite: 1].

### 2. Fundamental Scoring System (Skala 1-10)
Memberikan peringkat kualitas emiten berdasarkan 20+ rasio finansial[cite: 1]:
* **Profitabilitas & Efisiensi:** ROE, ROA, GPM, OPM, NPM, Asset Turnover[cite: 1].
* **Solvabilitas:** DER, DAR, *Interest Coverage*, *Net Debt/EBITDA*[cite: 1].
* **Kualitas Laba:** Rasio FCF terhadap *Net Income*[cite: 1].

### 3. Automated Data Compiler (Apps Script)
Otomatisasi penggabungan ribuan baris data antar sektor dengan fitur[cite: 1]:
* **Smart Detection:** Mendeteksi baris terakhir yang terisi secara dinamis untuk efisiensi eksekusi[cite: 1].
* **Data Cleaning:** Menghapus entri kosong dan memastikan konsistensi format data (Plain Text pada simbol emiten)[cite: 1].
* **Large Scale Management:** Mampu mengonsolidasi hingga 164 kolom data finansial secara instan[cite: 1].

## 📁 Struktur Repositori
* `scripts/python/`: Kode sumber utama untuk ekstraksi dan logika valuasi[cite: 1].
* `scripts/apps-script/`: File `.gs` berisi logika penggabungan (compiler) di Google Sheets[cite: 1].
* `data/`: Sampel data hasil akhir dalam format `.csv` (`COMPILE_VALUATION` & `COMPILE_DETAILS`)[cite: 1].

## 🛠 Tech Stack
* **Languages:** Python (Pandas, Numpy, YFinance) & JavaScript (Google Apps Script)[cite: 1].
* **Integration:** Google Sheets API & GSpread[cite: 1].
* **Environment:** Google Colab / Local Python Environment[cite: 1].

## 📝 Cara Penggunaan
1. Jalankan skrip Python di folder `scripts/python/` untuk mengisi data sektoral di Spreadsheet Anda[cite: 1].
2. Buka **Extensions > Apps Script** di Google Sheets Anda[cite: 1].
3. Salin kode dari `scripts/apps-script/` ke editor Apps Script[cite: 1].
4. Jalankan fungsi `compileValuationSheets` atau `compileDetailsSheets` untuk mendapatkan laporan konsolidasi[cite: 1].

---
*Disclaimer: Alat ini dibuat untuk tujuan edukasi dan analisis data, bukan merupakan ajakan jual atau beli saham tertentu.*
