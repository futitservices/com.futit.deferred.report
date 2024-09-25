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
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
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
import org.openbravo.dal.service.OBDal;
import org.openbravo.dal.core.OBContext;

public class ReportDeferredRevenueExpenses extends BaseProcessActionHandler {

  private static final Logger log = Logger.getLogger(ReportDeferredRevenueExpenses.class);
  private String tmpfileName;
  private static final String REVENUE_SHEET = "Deferred Revenue Report";
  private static final String EXPENSE_SHEET = "Deferred Expenses Report";
  private static final String BUSINESS_PARTNER_LBL = "Business Partner";
  private static final String INVOICE_NO_LBL = "Invoice No.";
  private static final String INVOICE_DESC_LBL = "Invoice Description";
  private static final String TOTAL_LBL = "Total";
  private static final String PARAMS = "_params";
  private static final String DATE_FORMAT_JAVA = "dateFormat.java";
  private static final String DATE_FORMAT_SQL = "dateTimeFormat.sql";
  private static final String FILE_NAME = "ReportDeferredRevenueExpenses.xlsx";

  @Override
  protected JSONObject doExecute(Map<String, Object> parameters, String content) {
    try {
      log.info("content.................. ::  " + content);
      JSONObject request = new JSONObject(content);
      JSONObject params = request.getJSONObject(PARAMS);
      String strBPartnerId = StringUtils.equals(params.getString("C_BPartner_ID"), "null") ? null : params.getString(
          "C_BPartner_ID");
      boolean isSale = params.getBoolean("IsSale");

      DateDomainType dateDomainType = new DateDomainType();
      Date startDateParam = (Date) dateDomainType.createFromString(params.getString("StartDate"));
      Date endDateParam = (Date) dateDomainType.createFromString(params.getString("EndDate"));
      String startDate = DateFormatUtils.format(startDateParam,
          OBPropertiesProvider.getInstance().getOpenbravoProperties().getProperty(DATE_FORMAT_JAVA));
      String endDate = DateFormatUtils.format(endDateParam,
          OBPropertiesProvider.getInstance().getOpenbravoProperties().getProperty(DATE_FORMAT_JAVA));
      // Blank workbook
      XSSFWorkbook workbook = new XSSFWorkbook();
      // Create a blank sheet
      XSSFSheet sheet = null;
      if (isSale) {
        sheet = workbook.createSheet(REVENUE_SHEET);
      } else {
        sheet = workbook.createSheet(EXPENSE_SHEET);
      }

      List<Object[]> invoiceDetails = getInvoiceDetails(strBPartnerId, startDate, endDate, isSale);
      if (invoiceDetails.size() > 0) {
        createRows(sheet, invoiceDetails, startDate, endDate);
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
    return buildDownloadResponse(parameters, content, true);
  }

  private static Date convertStringToDate(String dateStr) throws ParseException {
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
    return formatter.parse(dateStr);
  }

  private static List<Object[]> getInvoiceDetails(String bpartnerId, String startingDate, String endingDate,
      boolean salesTransaction) throws ParseException {

    Date startDate = convertStringToDate(startingDate);
    Date endDate = convertStringToDate(endingDate);
    // Build the base HQL query
    String hql = "select fa.businessPartner.name, il.invoice.documentNo, coalesce(il.invoice.description,'') as description, " +
        "to_char(fa.accountingDate, 'Mon-YYYY') as acctDate, fa.period.endingDate, ";
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

    // add group by clause to the HQL query
    hql += "group by fa.businessPartner.name, il.invoice.documentNo, il.invoice.description, to_char(fa.accountingDate, 'Mon-YYYY'), fa.period.endingDate ";
    
    // add order by clause to the HQL query
    hql += "order by il.invoice.documentNo, fa.period.endingDate ";

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

    // Execute the query and return the result list
    @SuppressWarnings("unchecked")
    List<Object[]> resultList = query.list();
    return resultList;
  }

  private static void createRows(XSSFSheet sheet, List<Object[]> invoiceDetails, String startDateStr,
      String endDateStr) {
    // Start row for invoice lines (after the header)
    int rowIdx = 1;

    // Date formatter to match the column headers
    SimpleDateFormat monthYearFormat = new SimpleDateFormat("MMM-yyyy", Locale.ENGLISH);
    SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy", Locale.ENGLISH);
    Calendar startDate = Calendar.getInstance();
    Calendar endDate = Calendar.getInstance();

    try {
      startDate.setTime(sdf.parse(startDateStr));
      endDate.setTime(sdf.parse(endDateStr));
    } catch (Exception e) {
      e.printStackTrace();
    }
    Map<String, Map<String, Object>> dataMap = new LinkedHashMap<>();
    // Process the query results
    for (Object[] result : invoiceDetails) {
      String businessPartner = (String) result[0];
      String invoiceNo = (String) result[1];
      String description = (String) result[2];
      String month = (String) result[3];
      BigDecimal amount = (BigDecimal) result[5];

      // Populate the data map
      dataMap.putIfAbsent(invoiceNo, new LinkedHashMap<>());
      Map<String, Object> invoiceData = dataMap.get(invoiceNo);
      invoiceData.putIfAbsent("businessPartner", businessPartner);
      invoiceData.putIfAbsent("invoiceDescription", description);

      // Store monthly values
      Map<String, BigDecimal> monthlyValues = (Map<String, BigDecimal>) invoiceData.getOrDefault("monthlyValues", new LinkedHashMap<String, BigDecimal>());
      monthlyValues.put(month, amount);
      invoiceData.put("monthlyValues", monthlyValues);
    }

    // Set header row
    Set<String> months = new LinkedHashSet<>();
    for (Map<String, Object> invoiceData : dataMap.values()) {
      Map<String, BigDecimal> monthlyValues = (Map<String, BigDecimal>) invoiceData.get("monthlyValues");
      months.addAll(monthlyValues.keySet());
    }
    XSSFCellStyle centerCellStyle = sheet.getWorkbook().createCellStyle();
    centerCellStyle.setAlignment(HorizontalAlignment.CENTER);
    XSSFCellStyle rightCellStyle = sheet.getWorkbook().createCellStyle();
    rightCellStyle.setAlignment(HorizontalAlignment.RIGHT);
    // Create the header row
    Row headerRow = sheet.createRow(0);
    headerRow.createCell(0).setCellValue(BUSINESS_PARTNER_LBL);
    headerRow.createCell(1).setCellValue(INVOICE_NO_LBL);
    headerRow.createCell(2).setCellValue(INVOICE_DESC_LBL);

    int colIndex = 3;
    for (String month : months) {
      Cell headerRowMonthCell = headerRow.createCell(colIndex++);
      headerRowMonthCell.setCellValue(month);
      headerRowMonthCell.setCellStyle(rightCellStyle);
    }
    Cell headerRowTotalCell = headerRow.createCell(colIndex);
    headerRowTotalCell.setCellValue(TOTAL_LBL);
    headerRowTotalCell.setCellStyle(rightCellStyle);

    // Write the data rows
    int rowIndex = 1;
    for (Map.Entry<String, Map<String, Object>> entry : dataMap.entrySet()) {
      Row row = sheet.createRow(rowIndex++);

      String invoiceNo = entry.getKey();
      Map<String, Object> invoiceData = entry.getValue();
      String businessPartner = (String) invoiceData.get("businessPartner");
      String invoiceDescription = (String) invoiceData.get("invoiceDescription");
      Map<String, BigDecimal> monthlyValues = (Map<String, BigDecimal>) invoiceData.get("monthlyValues");

      // Business partner, invoiceNo, and invoiceDescription
      row.createCell(0).setCellValue(businessPartner);
      row.createCell(1).setCellValue(invoiceNo);
      row.createCell(2).setCellValue(invoiceDescription);

      // Write amounts for each month and calculate the total
      colIndex = 3;
      BigDecimal total = BigDecimal.ZERO;
      for (String month : months) {
        BigDecimal amount = monthlyValues.getOrDefault(month, BigDecimal.ZERO);
        Cell cell = row.createCell(colIndex++);
        cell.setCellValue(amount.doubleValue());
        cell.setCellStyle(rightCellStyle);
        total = total.add(amount);
      }

      // Write the total value
      Cell totalCell = row.createCell(colIndex);
      totalCell.setCellValue(total.doubleValue());
      totalCell.setCellStyle(rightCellStyle);
    }
  }

  protected JSONObject buildDownloadResponse(Map<String, Object> parameters, String content,
      boolean isExport) {
    JSONObject result = new JSONObject();
    try {
      final JSONArray actions = new JSONArray();
      actions.put(0, buildReportAction(parameters, content, isExport));
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
