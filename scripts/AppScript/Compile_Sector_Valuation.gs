function compileValuationSheets() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const targetSheetName = "COMPILE_VALUATION";
  
  // 1. Siapkan Sheet Target (COMPILE_VALUATION)
  let targetSheet = ss.getSheetByName(targetSheetName);
  if (!targetSheet) {
    targetSheet = ss.insertSheet(targetSheetName);
  } else {
    targetSheet.clear(); 
  }

  // 2. Daftar konfigurasi sheet
  const sheetsToStack = [
    { name: "IDXTECHNO_VALUATION", startRow: 1 }, 
    { name: "IDXNONCYC_VALUATION", startRow: 2 },
    { name: "IDXCYCLIC_VALUATION", startRow: 2 },
    { name: "IDXINDUST_VALUATION", startRow: 2 },
    { name: "IDXFINANCE_VALUATION", startRow: 2 },
    { name: "IDXBASIC_VALUATION", startRow: 2 },
    { name: "IDXENERGY_VALUATION", startRow: 2 },
    { name: "IDXHEALTH_VALUATION", startRow: 2 },
    { name: "IDXINFRA_VALUATION", startRow: 2 },
    { name: "IDXTRANS_VALUATION", startRow: 2 }
  ];

  let compiledData = [];

  // 3. Looping ke setiap sheet
  sheetsToStack.forEach(config => {
    let sheet = ss.getSheetByName(config.name);
    
    if (sheet) {
      // --- PENDETEKSI REAL LAST ROW BERDASARKAN KOLOM A ---
      let colA = sheet.getRange("A:A").getValues();
      let realLastRow = 0;
      
      for (let r = colA.length - 1; r >= 0; r--) {
        if (colA[r][0].toString().trim() !== "") {
          realLastRow = r + 1; 
          break;
        }
      }
      // ----------------------------------------------------

      if (realLastRow >= config.startRow) {
        let numRows = realLastRow - config.startRow + 1;
        
        // Tarik data HANYA sampai baris terakhir yang terisi (A sampai AK = 37 kolom)
        let data = sheet.getRange(config.startRow, 1, numRows, 37).getValues();
        
        // Filter ekstra: Buang jika di tengah-tengah ada baris yang kolom A-nya kosong
        let filteredData = data.filter(row => row[0].toString().trim() !== "");
        
        if (filteredData.length > 0) {
          compiledData = compiledData.concat(filteredData);
          Logger.log(`[SUKSES] Menarik ${filteredData.length} baris dari ${config.name} (Terbaca sampai baris ke-${realLastRow})`);
        }
      } else {
        Logger.log(`[SKIP] Sheet ${config.name} kosong (tidak ada data di bawah baris start).`);
      }
    } else {
      Logger.log(`[SKIP] Sheet ${config.name} tidak ditemukan. Diabaikan.`);
    }
  });

  // 4. Tulis semua data ke sheet target (COMPILE_VALUATION) sekaligus
  if (compiledData.length > 0) {
    // KUNCI FORMAT KOLOM E SEBAGAI TEXT (Simbol "@" di Apps Script berarti Plain Text)
    // Kolom E adalah kolom ke-5
    targetSheet.getRange("E:E").setNumberFormat("@");

    // Tulis Data
    targetSheet.getRange(1, 1, compiledData.length, compiledData[0].length).setValues(compiledData);
    Logger.log(`Berhasil menggabungkan total ${compiledData.length} baris ke sheet ${targetSheetName}.`);
  } else {
    Logger.log(`Peringatan: Tidak ada data valid yang ditemukan untuk digabung.`);
  }
}
