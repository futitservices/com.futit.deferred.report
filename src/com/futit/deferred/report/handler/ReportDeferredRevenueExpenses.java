package com.futit.deferred.report.handler;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.hibernate.query.Query;
import org.openbravo.base.exception.OBException;
import org.openbravo.base.model.domaintype.DateDomainType;
import org.openbravo.base.session.OBPropertiesProvider;
import org.openbravo.client.application.process.BaseProcessActionHandler;
import org.openbravo.client.application.report.BaseReportActionHandler;
import org.openbravo.client.application.report.ReportingUtils;
import org.openbravo.dal.core.OBContext;
import org.openbravo.dal.service.OBDal;
import org.openbravo.erpCommon.businessUtility.Preferences;
import org.openbravo.erpCommon.utility.PropertyException;
import org.openbravo.model.ad.system.Client;
import org.openbravo.model.common.enterprise.Organization;

public class ReportDeferredRevenueExpenses extends BaseProcessActionHandler {

  private static final Logger log = Logger.getLogger(ReportDeferredRevenueExpenses.class);
  private String tmpfileName;
  private static final String REVENUE_SHEET = "Deferred Revenue Report";
  private static final String EXPENSE_SHEET = "Deferred Expenses Report";
  private static final String BUSINESS_PARTNER_LBL = "Business Partner";
  private static final String INVOICE_NO_LBL = "Invoice No.";
  private static final String INVOICE_DESC_LBL = "Invoice Description";
  private static final String TOTAL_LBL = "Total";
  private static final String REVENUE_LBL = "Deferred Revenue";
  private static final String EXPENSE_LBL = "Deferred Expense";
  private static final String SUMMARY_LBL = "Deferred Revenue and Expense Summary";
  private static final String PARAMS = "_params";
  private static final String DATE_FORMAT_JAVA = "dateFormat.java";
  private static final String FILE_NAME = "Deferred-Revenue-Expenses-Report.xlsx";
  private static final String END_CUSTOMER_LBL = "End Customer";
  private static final String END_CUSTOMER_ENABLED = "FUTDRER_EndCustomer";

  @Override
  protected JSONObject doExecute(Map<String, Object> parameters, String content) {
    try {
      log.info("content.................. ::  " + content);
      log.info("endCustomerEnabled.................. ::  " + endCustomerEnabled());
      JSONObject request = new JSONObject(content);
      JSONObject params = request.getJSONObject(PARAMS);
      String strBPartnerId = StringUtils.equals(params.getString("C_BPartner_ID"), "null") ? null : params.getString(
          "C_BPartner_ID");
      String endCustomer = "";
      if (endCustomerEnabled()) {
        endCustomer = StringUtils.equals(params.getString("EM_Futdrer_Endcustomer"), "null") ? null : params.getString(
            "EM_Futdrer_Endcustomer");
      }
      boolean isSale = params.getBoolean("IsSale");
      boolean bothSalesPurchase = params.getBoolean("IsBoth");
      boolean isSummary = params.getBoolean("IsSummary");

      DateDomainType dateDomainType = new DateDomainType();
      Date startDateParam = (Date) dateDomainType.createFromString(params.getString("StartDate"));
      Date endDateParam = (Date) dateDomainType.createFromString(params.getString("EndDate"));
      String startDate = DateFormatUtils.format(startDateParam,
          OBPropertiesProvider.getInstance().getOpenbravoProperties().getProperty(DATE_FORMAT_JAVA));
      String endDate = DateFormatUtils.format(endDateParam,
          OBPropertiesProvider.getInstance().getOpenbravoProperties().getProperty(DATE_FORMAT_JAVA));
      // Blank workbook
      XSSFWorkbook workbook = new XSSFWorkbook();

      // Generate sheets based on combinations
      if (isSale && !bothSalesPurchase) {
        XSSFSheet revenueSheet = workbook.createSheet(REVENUE_SHEET);
        List<Object[]> invoiceDetails = getInvoiceDetails(strBPartnerId, endCustomer, startDate, endDate, true);
        createRows(revenueSheet, invoiceDetails, startDate, endDate);
      } else if (!isSale && !bothSalesPurchase) {
        XSSFSheet expenseSheet = workbook.createSheet(EXPENSE_SHEET);
        List<Object[]> invoiceDetails = getInvoiceDetails(strBPartnerId, endCustomer, startDate, endDate, false);
        createRows(expenseSheet, invoiceDetails, startDate, endDate);
      } else if (bothSalesPurchase) {
        XSSFSheet revenueSheet = workbook.createSheet(REVENUE_SHEET);
        List<Object[]> revenueDetails = getInvoiceDetails(strBPartnerId, endCustomer, startDate, endDate, true);
        createRows(revenueSheet, revenueDetails, startDate, endDate);

        XSSFSheet expenseSheet = workbook.createSheet(EXPENSE_SHEET);
        List<Object[]> expenseDetails = getInvoiceDetails(strBPartnerId, endCustomer, startDate, endDate, false);
        createRows(expenseSheet, expenseDetails, startDate, endDate);

        if (isSummary) {
          XSSFSheet summarySheet = workbook.createSheet(TOTAL_LBL);
          createSummarySheet(summarySheet, revenueDetails, expenseDetails, SUMMARY_LBL);
        }
      }

      // this Writes the workbook
      FileOutputStream out = new FileOutputStream(
          new File(ReportingUtils.getTempFolder() + "/" + getTmpfileName()));
      workbook.write(out);
      out.close();

    } catch (JSONException | IOException e) {
      throw new OBException("Unexpected data format", e);
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
    return buildDownloadResponse(parameters, content);
  }

  private static List<Object[]> getInvoiceDetails(String bpartnerId, String endCustomer, String startingDate,
      String endingDate, boolean salesTransaction) throws ParseException {

    // Build the base HQL query
    String hql = "select fa.businessPartner.name, il.invoice.documentNo, coalesce(il.invoice.description,'') as description, " +
        "to_char(fa.accountingDate, 'Mon-YYYY') as acctDate, fa.period.endingDate, ";
    if (endCustomerEnabled()) {
      hql += "il.invoice.futdrerEndcustomer as endcustomer, ";
    }
    // If transaction is sales or purchase, add it to the HQL query
    if (salesTransaction) {
      hql += "sum(fa.debit) as amount ";
    } else {
      hql += "sum(fa.credit) as amount ";
    }

    hql += "from FinancialMgmtAccountingFact fa join fa.product as p join p.productAccountsList as pal join InvoiceLine il on il.id=fa.lineID ";

    // If transaction is sales or purchase, add it to the HQL query
    if (salesTransaction) {
      hql += "join pal.productDeferredRevenue as cvc ";
    } else {
      hql += "join pal.defExpenseAcct as cvc ";
    }

    hql += "where pal.accountingSchema.id = (select distinct(o.generalLedger.id) from Organization o where ad_isorgincluded(il.invoice.organization.id, o.id, o.client) <> -1 and o.generalLedger is not null) " +
        "and fa.client.id = :clientId " +
        "and cvc.account=fa.account and il.invoice.documentType.return = false and il.deferred = true and il.invoice.salesTransaction = :salesTransaction " +
        "and fa.accountingDate between to_date(:startDate) and to_date(:endDate) ";

    // If transaction is sales or purchase, add it to the HQL query
    if (salesTransaction) {
      hql += "and fa.debit > 0 ";
    } else {
      hql += "and fa.credit > 0 ";
    }

    // If Business Partner ID is provided, add it to the HQL query
    if (bpartnerId != null && !bpartnerId.isEmpty()) {
      hql += "and fa.businessPartner.id = :bpartnerId ";
    }

    // If End Customer is provided, add it to the HQL query
    if (endCustomerEnabled()) {
      if (endCustomer != null && !endCustomer.isEmpty()) {
        hql += "and il.invoice.futdrerEndcustomer = :endCustomer ";
      }
    }

    // add group by clause to the HQL query
    hql += "group by fa.businessPartner.name, ";

    if (endCustomerEnabled()) {
      hql += "il.invoice.futdrerEndcustomer, ";
    }

    hql += "il.invoice.documentNo, il.invoice.description, to_char(fa.accountingDate, 'Mon-YYYY'), fa.period.endingDate ";
    // add order by clause to the HQL query
    hql += "order by fa.period.endingDate, ";

    if (endCustomerEnabled()) {
      hql += "il.invoice.futdrerEndcustomer, ";
    }

    hql += "il.invoice.documentNo ";

    // Create the query object
    Query query = OBDal.getInstance().getSession().createQuery(hql);

    // Set mandatory parameters
    query.setParameter("clientId", OBContext.getOBContext().getCurrentClient().getId());
    query.setParameter("salesTransaction", salesTransaction);
    query.setParameter("startDate", startingDate);
    query.setParameter("endDate", endingDate);

    // Set optional Business Partner parameter if provided
    if (bpartnerId != null && !bpartnerId.isEmpty()) {
      query.setParameter("bpartnerId", bpartnerId);
    }

    // Set optional end customer parameter if provided
    if (endCustomerEnabled()) {
      if (endCustomer != null && !endCustomer.isEmpty()) {
        query.setParameter("endCustomer", endCustomer);
      }
    }

    // Execute the query and return the result list
    @SuppressWarnings("unchecked")
    List<Object[]> resultList = query.list();
    return resultList;
  }

  private static void createRows(XSSFSheet sheet, List<Object[]> invoiceDetails, String startDateStr,
      String endDateStr) {
    int rowIdx = 1; // Start row for invoice lines (after the header)
    String endCustomer = "";
    BigDecimal amount = BigDecimal.ZERO;

    SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy", Locale.ENGLISH);
    Calendar startDate = Calendar.getInstance();
    Calendar endDate = Calendar.getInstance();
    try {
      startDate.setTime(sdf.parse(startDateStr));
      endDate.setTime(sdf.parse(endDateStr));
    } catch (Exception e) {
      e.printStackTrace();
    }
    // Bold Style Font
    XSSFCellStyle boldCellStyle = sheet.getWorkbook().createCellStyle();
    XSSFFont boldFont = sheet.getWorkbook().createFont();
    boldFont.setBold(true);
    boldFont.setFontHeightInPoints((short) 10); // Set font size to 10 pt
    boldCellStyle.setFont(boldFont);

    Map<String, Map<String, Object>> dataMap = new LinkedHashMap<>();

    // Process the query results
    for (Object[] result : invoiceDetails) {
      String businessPartner = (String) result[0];
      String invoiceNo = (String) result[1];
      String description = (String) result[2];
      String month = (String) result[3];
      if (endCustomerEnabled()) {
        endCustomer = (String) result[5];
        amount = (BigDecimal) result[6];
      } else {
        amount = (BigDecimal) result[5];
      }
      dataMap.putIfAbsent(invoiceNo, new LinkedHashMap<>());
      Map<String, Object> invoiceData = dataMap.get(invoiceNo);
      invoiceData.putIfAbsent("businessPartner", businessPartner);
      invoiceData.putIfAbsent("invoiceNo", invoiceNo);
      if (endCustomerEnabled()) {
        invoiceData.putIfAbsent("endCustomer", endCustomer);
      }
      invoiceData.putIfAbsent("invoiceDescription", description);

      Map<String, BigDecimal> monthlyValues = (Map<String, BigDecimal>) invoiceData.getOrDefault("monthlyValues",
          new LinkedHashMap<>());
      monthlyValues.put(month, amount);
      invoiceData.put("monthlyValues", monthlyValues);
    }

    Set<String> months = new LinkedHashSet<>();
    for (Map<String, Object> invoiceData : dataMap.values()) {
      Map<String, BigDecimal> monthlyValues = (Map<String, BigDecimal>) invoiceData.get("monthlyValues");
      months.addAll(monthlyValues.keySet());
    }

    XSSFCellStyle centerCellStyle = sheet.getWorkbook().createCellStyle();
    centerCellStyle.setAlignment(HorizontalAlignment.CENTER);

    XSSFCellStyle rightCellStyle = sheet.getWorkbook().createCellStyle();
    rightCellStyle.setAlignment(HorizontalAlignment.RIGHT);

    // Header row
    Row headerRow = sheet.createRow(0);
    headerRow.createCell(0).setCellValue(BUSINESS_PARTNER_LBL);
    headerRow.createCell(1).setCellValue(INVOICE_NO_LBL);
    headerRow.createCell(2).setCellValue(INVOICE_DESC_LBL);

    if (endCustomerEnabled()) {
      headerRow.createCell(3).setCellValue(END_CUSTOMER_LBL);
    }

    int monthIdx = endCustomerEnabled() ? 4 : 3;
    for (String month : months) {
      headerRow.createCell(monthIdx++).setCellValue(month);
    }

    headerRow.createCell(monthIdx).setCellValue(TOTAL_LBL);

    // Populate rows
    BigDecimal[] monthlyTotals = new BigDecimal[months.size()];
    for (int i = 0; i < monthlyTotals.length; i++) {
      monthlyTotals[i] = BigDecimal.ZERO;
    }
    int totalColIdx = endCustomerEnabled() ? 4 : 3;
    for (Map<String, Object> invoiceData : dataMap.values()) {
      Row row = sheet.createRow(rowIdx++);
      row.createCell(0).setCellValue((String) invoiceData.get("businessPartner"));
      row.createCell(1).setCellValue((String) invoiceData.get("invoiceNo"));
      row.createCell(2).setCellValue((String) invoiceData.get("invoiceDescription"));
      if (endCustomerEnabled()) {
        row.createCell(3).setCellValue((String) invoiceData.get("endCustomer"));
      }
      Map<String, BigDecimal> monthlyValues = (Map<String, BigDecimal>) invoiceData.get("monthlyValues");

      int colIdx = totalColIdx;
      BigDecimal rowTotal = BigDecimal.ZERO;
      for (String month : months) {
        BigDecimal monthValue = monthlyValues.getOrDefault(month, BigDecimal.ZERO);
        row.createCell(colIdx).setCellValue(monthValue.doubleValue());
        rowTotal = rowTotal.add(monthValue);
        monthlyTotals[colIdx - totalColIdx] = monthlyTotals[colIdx - totalColIdx].add(monthValue);
        colIdx++;
      }

      row.createCell(colIdx).setCellValue(rowTotal.doubleValue());
    }

    // Add total row
    Row totalRow = sheet.createRow(rowIdx);
    Cell totalRowCell = totalRow.createCell(0);
    totalRowCell.setCellValue(TOTAL_LBL);
    totalRowCell.setCellStyle(boldCellStyle);


    int colIdx = totalColIdx;
    for (BigDecimal monthlyTotal : monthlyTotals) {
      Cell totalCell = totalRow.createCell(colIdx++);
      totalCell.setCellValue(monthlyTotal.doubleValue());
      totalCell.setCellStyle(boldCellStyle);
    }

    // Add grand total
    BigDecimal grandTotal = BigDecimal.ZERO;
    for (BigDecimal monthlyTotal : monthlyTotals) {
      grandTotal = grandTotal.add(monthlyTotal);
    }
    Cell grandTotalCell = totalRow.createCell(colIdx);
    grandTotalCell.setCellValue(grandTotal.doubleValue());
    grandTotalCell.setCellStyle(boldCellStyle);
  }

  private static void createSummarySheet(XSSFSheet summarySheet, List<Object[]> revenueDetails,
      List<Object[]> expenseDetails, String summaryType) {
    int colIdx = 1; // Start from the second column to allow the first column for labels (Deferred Revenue, etc.)

    XSSFCellStyle boldCellStyle = summarySheet.getWorkbook().createCellStyle();
    XSSFFont boldFont = summarySheet.getWorkbook().createFont();
    boldFont.setBold(true);
    boldFont.setFontHeightInPoints((short) 10); // Set font size to 10 pt
    boldCellStyle.setFont(boldFont);

    // Map to store month-year data for deferred revenue and expense
    Map<String, BigDecimal> revenueMap = new LinkedHashMap<>();
    Map<String, BigDecimal> expenseMap = new LinkedHashMap<>();

    // Populate the revenue map
    for (Object[] result : revenueDetails) {
      String monthYear = (String) result[3];
      BigDecimal amount = (BigDecimal) result[6];
      revenueMap.put(monthYear, revenueMap.getOrDefault(monthYear, BigDecimal.ZERO).add(amount));
    }

    // Populate the expense map
    for (Object[] result : expenseDetails) {
      String monthYear = (String) result[3];
      BigDecimal amount = (BigDecimal) result[6];
      expenseMap.put(monthYear, expenseMap.getOrDefault(monthYear, BigDecimal.ZERO).add(amount));
    }

    // Determine all unique months
    Set<String> allMonths = new LinkedHashSet<>();
    allMonths.addAll(revenueMap.keySet());
    allMonths.addAll(expenseMap.keySet());

    // Create the header row with months
    Row headerRow = summarySheet.createRow(0);
    headerRow.createCell(0).setCellValue("");
    for (String monthYear : allMonths) {
      Cell cell = headerRow.createCell(colIdx++);
      cell.setCellValue(monthYear);
    }

    // Row for Deferred Revenue
    Row revenueRow = summarySheet.createRow(1);
    revenueRow.createCell(0).setCellValue(REVENUE_LBL);
    colIdx = 1;
    for (String monthYear : allMonths) {
      BigDecimal revenue = revenueMap.getOrDefault(monthYear, BigDecimal.ZERO);
      revenueRow.createCell(colIdx++).setCellValue(revenue.doubleValue());
    }

    // Row for Deferred Expense
    Row expenseRow = summarySheet.createRow(2);
    expenseRow.createCell(0).setCellValue(EXPENSE_LBL);
    colIdx = 1;
    for (String monthYear : allMonths) {
      BigDecimal expense = expenseMap.getOrDefault(monthYear, BigDecimal.ZERO);
      expenseRow.createCell(colIdx++).setCellValue(expense.doubleValue());
    }

    // Row for Difference
    Row differenceRow = summarySheet.createRow(3);
    Cell differenceRowCell = differenceRow.createCell(0);
    differenceRowCell.setCellValue(TOTAL_LBL);
    differenceRowCell.setCellStyle(boldCellStyle);
    colIdx = 1;
    for (String monthYear : allMonths) {
      BigDecimal revenue = revenueMap.getOrDefault(monthYear, BigDecimal.ZERO);
      BigDecimal expense = expenseMap.getOrDefault(monthYear, BigDecimal.ZERO);
      BigDecimal difference = revenue.subtract(expense);
      Cell differenceRowCells = differenceRow.createCell(colIdx++);
      differenceRowCells.setCellValue(difference.doubleValue());
      differenceRowCells.setCellStyle(boldCellStyle);
    }
  }

  private static boolean endCustomerEnabled() {
    boolean endCustomerEnabled = false;
    Client client = OBDal.getInstance().get(Client.class, OBContext.getOBContext().getCurrentClient().getId());
    Organization organization = OBDal.getInstance().get(Organization.class, "0");
    try {
      endCustomerEnabled = Preferences
          .getPreferenceValue(END_CUSTOMER_ENABLED, true,
              client, organization, null, null, null)
          .equals("Y");
    } catch (PropertyException e) {
      log.error(e);
    }
    return endCustomerEnabled;
  }

  protected JSONObject buildDownloadResponse(Map<String, Object> parameters, String content) {
    JSONObject result = new JSONObject();
    try {
      final JSONArray actions = new JSONArray();
      actions.put(0, buildReportAction(parameters, content, true));
      result.put("responseActions", actions);
    } catch (JSONException ignore) {
    }
    return result;
  }

  private JSONObject buildReportAction(Map<String, Object> parameters, String content,
      boolean isExport) throws JSONException {
    final JSONObject reportAction = new JSONObject();
    reportAction.put("OBUIAPP_downloadReport", buildRecordInfo(parameters, content));
    JSONObject msgInBPTab = new JSONObject();
    msgInBPTab.put("msgType", "success");
    msgInBPTab.put("msgTitle", "Process execution");
    msgInBPTab.put("msgText", "This record was opened from process execution");
    reportAction.put("showMsgInView", msgInBPTab);

    if (isExport) {
      reportAction.put("refreshGrid", new JSONObject());
    }

    return reportAction;
  }

  private JSONObject buildRecordInfo(Map<String, Object> parameters, String content)
      throws JSONException {
    final JSONObject recordInfo = new JSONObject();
    recordInfo.put("processParameters", buildParams(parameters, content));
    recordInfo.put("tmpfileName", getTmpfileName());
    recordInfo.put("fileName", FILE_NAME);
    return recordInfo;
  }

  private JSONObject buildParams(Map<String, Object> parameters, String content)
      throws JSONException {
    JSONObject jsonData = new JSONObject(content);
    JSONObject params = jsonData.getJSONObject("_params");
    params.put("processId", parameters.get("processId"));
    params.put("reportId", parameters.get("reportId"));
    params.put("actionHandler", new BaseReportActionHandler().getClass().getName());
    return params;
  }

  private String getTmpfileName() {
    if (tmpfileName != null) {
      return tmpfileName;
    }
    String name = UUID.randomUUID().toString();
    tmpfileName = name + ".xlsx";

    return tmpfileName;
  }
}
