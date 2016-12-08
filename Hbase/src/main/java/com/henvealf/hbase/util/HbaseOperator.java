package com.henvealf.hbase.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * HBase 操作表类
 */
public class HbaseOperator {
	public Configuration conf;
	public Connection conn;
	public Admin admin;
	
	public HbaseOperator() {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum","localhost");
		conf.set("hbase.zookeeper.property.clientPort","2182");
		conf.set("zookeeper.znode.parent","/hbase");  
		try {
            // 使用配置生成一个连接
			conn = ConnectionFactory.createConnection(conf);
            // 在连接中获取管理员对象
			admin = conn.getAdmin();
			System.out.println("hbase连接成功！");
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("hbase连接失败！");
		}
	}
	
	
	
	public static void main(String[] args) throws IOException {
		HbaseOperator hbaseOperate = new HbaseOperator();
//		String[] cf = {"basic","accesory"};
		String tableNmae = "hp_test";
//		hbaseOperate.createTable(tableNmae, cf);
		
//		hbaseOperate.insterRow(tableNmae, "110:xiaoming", "basic", "age", "30");
		
		
//		hbaseOperate.getData(tableNmae, "110:xiaoming", "basic", "age");
//		
//		hbaseOperate.deleRow(tableNmae, "110:xiaoming", "basic", "age");
//		
		hbaseOperate.regionInfo(tableNmae);
//		
//		hbaseOperate.deleteTable(tableNmae);
//		hbaseOperate.listTables();
		hbaseOperate.close();
	}
	
	//建表  
    public void createTable(String tableName,String... colFamiliesName){

        // 构造一个 表名
        TableName table = TableName.valueOf(tableName);
        try {
			if(admin.tableExists(table)){
                System.err.println("table "+table+" is exists!");
            }else {
                if (colFamiliesName.length == 0) {
                    System.err.println("缺少列族！！");
                    return;
                }
                // 描述符，构造参数为表明，指明是哪个表的描述符
			    HTableDescriptor hTableDescriptor = new HTableDescriptor(table);
			    for(String familyName:colFamiliesName){
                    // HColumnDescriptor 代表一个列族的描述符，构造函数是列族的名字
			        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(familyName);
                    // 像表描述符中添加列族的描述符
			        hTableDescriptor.addFamily(hColumnDescriptor);  
			    }
			    // 最后使用管理员来创建表
			    admin.createTable(hTableDescriptor); 
			    System.out.println("hbase表： "+table+" 创建成功！");
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("hbase表： "+table+"创建失败！");
		}  
    }
    
    
    //删表  
    public void deleteTable(String tableName) throws IOException {  
        TableName tn = TableName.valueOf(tableName);  
        if (admin.tableExists(tn)) {  
            admin.disableTable(tn);  
            admin.deleteTable(tn);
            System.out.println("hbase表： "+tn+" 删除成功！");
        }  
    }  
    
    
    //查看已有表  
    public void listTables() throws IOException {
        // 获取当前 HBase 中所有表的描述符
        HTableDescriptor hTableDescriptors[] = admin.listTables();  
        for(HTableDescriptor hTableDescriptor :hTableDescriptors){  
            System.out.println("hbase中存在的表: "+hTableDescriptor.getNameAsString());  
        }  
    }  
    
    //插入数据  
    public void insertRow(String tableName,String rowkey,String colFamily,String col,String val) throws IOException {

        // 使用表明得到一个表
        Table table = conn.getTable(TableName.valueOf(tableName));

        Put put = new Put(Bytes.toBytes(rowkey));
        
        put.addColumn(
                    Bytes.toBytes(colFamily),
                    Bytes.toBytes(col),
                    Bytes.toBytes(val)
                );

        table.put(put);  
  
        System.out.println(rowkey+" 成功插入hbase表："+ tableName);
        //批量插入  
        List<Put> putList = new ArrayList<Put>(); 
        putList.add(put); 
        table.put(putList);  
        table.close();  
    }

    // 添加多行
    public void insertRows(String tableName, List<Put> putList) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        table.put(putList);
        table.close();
    }


    public void deleteRow(String tableName,String rowkey) throws IOException {
        deleteRow(tableName, rowkey, null, null);
    }

    public void deleteRow(String tableName,String rowkey, String colFamily) throws IOException {
        deleteRow(tableName, rowkey, colFamily, null);
    }

    /**
     * 删除数据
     */
    public void deleteRow(String tableName,String rowkey,String colFamily,String col) throws IOException {

        Table table = conn.getTable(TableName.valueOf(tableName));  
        Delete delete = new Delete(Bytes.toBytes(rowkey));

        //删除指定列族  
        if(colFamily != null) {
            delete.addFamily(Bytes.toBytes(colFamily));
        }

        //删除指定列
        if(colFamily != null && col != null) {
            delete.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
        }

        table.delete(delete);
        
        System.out.println(rowkey+"成功从hbase表："+tableName+"删除");
        table.close();  
    }

    /**
     * 批量删除
     * @param tableName
     * @param deleteList
     * @throws IOException
     */
    public void deleteRows (String tableName, List<Delete> deleteList) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        table.delete(deleteList);
        table.close();
    }

    //根据rowkey查找数据  
    public Cell[] getAllCells(String tableName,String rowkey,String colFamily,String col)throws  IOException{
        Table table = conn.getTable(TableName.valueOf(tableName));

        Get get = new Get(Bytes.toBytes(rowkey));  

        get.addColumn(
                        Bytes.toBytes(colFamily),
                        Bytes.toBytes(col)
                    );

        Result result = table.get(get);  
  
        if (result.isEmpty()) {
			System.out.println("rowkey: "+ rowkey +"不存在！");
            table.close();
		    return null;
        } else {
            Cell[] cells = result.rawCells();
            table.close();
            return cells;
		}
    }

    public Cell getNewerCell(String tableName,String rowkey,String colFamily,String col) throws IOException {
        Cell[] cells =  getAllCells(tableName, rowkey, colFamily, col);
        return cells == null ? null : cells[0];
    }
  
    //格式化输出  
    public void showCell(Result result){  
        Cell[] cells = result.rawCells();  
        for(Cell cell:cells){  
            System.out.println("RowName:"+new String(CellUtil.cloneRow(cell))+" ");  
            System.out.println("Timetamp:"+cell.getTimestamp()+" ");  
            System.out.println("column Family:"+new String(CellUtil.cloneFamily(cell))+" ");  
            System.out.println("row Name:"+new String(CellUtil.cloneQualifier(cell))+" ");  
            System.out.println("value:"+new String(CellUtil.cloneValue(cell))+" ");  
        }  
    }

    //批量查找数据
    public List<Result> scanDataResultList(String tableName)throws IOException{
        return scanDataResultList(tableName, null, null);
    }

    //批量查找数据  
    public List<Result> scanDataResultList(String tableName,String startRow,String stopRow)throws IOException{
        List<Result> resultList = new ArrayList<Result>();
        Table table = conn.getTable(TableName.valueOf(tableName));  
        Scan scan = new Scan();
        if (startRow != null)
            scan.setStartRow(Bytes.toBytes(startRow));
        if (stopRow != null)
            scan.setStopRow(Bytes.toBytes(stopRow));
        ResultScanner resultScanner = table.getScanner(scan);  
        for(Result result : resultScanner){
            resultList.add(result);
        }
        table.close();
        return resultList;
    }  
    
    //获取region信息
    public void regionInfo(String tableName) throws IOException{
    	RegionLocator regionLocator = conn.getRegionLocator(TableName.valueOf(tableName));
    	List<HRegionLocation> allRegionLocations = regionLocator.getAllRegionLocations();
    	for (HRegionLocation hRegionLocation : allRegionLocations) {
			System.out.println(hRegionLocation);
		}
    	System.out.println("RegionNum: "+ allRegionLocations.size());
    }
    
    //关闭链接
    public void close() {
		try {
			if (null!= admin) {
				admin.close();
			}
			if (null!= conn) {
				conn.close();
			}
			System.out.println("hbase链接成功关闭！");
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("hbase链接关闭失败！");
		}
	}

}
