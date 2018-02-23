#-*- coding:utf-8 -*-
import xlrd
import xlwt
import xlsxwriter 
import sys,os.path
import sys

reload(sys)
sys.setdefaultencoding('utf-8')
#pynlpir.open()

fname = "/Users/wangdi/Documents/upload/new_bai.xls"
outname = "outfile.txt"

book = xlrd.open_workbook(fname)
cd = []
#获得excel的book对象  
book = xlrd.open_workbook(fname)
#通过sheet索引获得sheet对象  
sh=book.sheet_by_index(0)
#获取行数
nrows = sh.nrows
#print nrows
#获取列数
ncols = sh.ncols
#print ncols
outputs=[]
for nd in range(nrows):
    try:
        #从Excel中获取原始数据
        zn = sh.cell_value(rowx=nd,colx=0)
        if zn == "":
            flag=flag+1
        else:
            ct = "\"" + zn + "\""
            outputs.append(ct)
            cd.append(nd)
    except:
        continue
print ',\n'.join(outputs)
print '\n',

def str_splic(col):
    cd.append(nrows)
    qt = []
    for cc in range(len(cd)-1):
        dt = []
        for dd in range(cd[cc+1] - cd[cc]):
            cn = sh.cell_value(rowx=cd[cc]+dd,colx=col)
            if col == 3:
                dt.append("%d"%int(cn))
            elif col == 5:
                dt.append("\"%s\""%(cn))
        qq = '{' + ','.join(dt) + '}'
        qt.append(qq)
    print ',\n'.join(qt),
    print '\n'
str_splic(3)
str_splic(5)
