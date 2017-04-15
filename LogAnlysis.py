import sys, re

#sys.argv=['-q', '1', 'a/iliad', 'a/odyssey']
#execfile('b/logAnalyser.py')

def main():
	qno = sys.argv[1] 
	hostDir1 = sys.argv[2] 
        hostDir2= sys.argv[3] 
	hosts = []
	hosts.append(os.path.basename(hostDir1))
	hosts.append(os.path.basename(hostDir2))

	if qno == '1':
		
		regex ='.*?(?:[a-z][a-z]+).*?((?:[a-z][a-z]+))([\s\S]+[\w\W]+[\d\D])'
	 	rg = re.compile(regex,re.IGNORECASE|re.DOTALL)
		ls = sc.textFile(hostDir1 + ',' + hostDir2)
		maps = ls.map(lambda x: None if rg.match(x) is None else (rg.match(x).group(1), 1))
		
		m = maps.filter(lambda x: x is not None and x[0] in hosts)
		hosts=m.reduceByKey(lambda x,y: x+y)
		show("Q1: Line counts")
		hosts.foreach(lambda x: show("	+ " + x[0] +": " + str(x[1])))
	
	if qno == '2':
		
                regex='.*?(?:[a-z][a-z]+).*?((?:[a-z][a-z]+)).*?(?:[a-z][a-z]+).*?(?:[a-z][a-z]+).*?(?:[a-z][a-z]+).*?(?:[a-z][a-z]+).*?(?:[a-z][a-z]+).*?((?:[a-z][a-z]+))'
		rg = re.compile(regex,re.IGNORECASE|re.DOTALL)
		ls = sc.textFile(hostDir1 + ',' + hostDir2)
		maps = ls.map(lambda x: None if rg.match(x) is None else (rg.match(x).group(1), rg.match(x).group(2)))
		
		m = maps.filter(lambda x: x is not None and x[0] in hosts and x[1] in 'achille')
		pairs = m.map(lambda x: (x[0],1))
		final= pairs.reduceByKey(lambda x,y: x + y)
		show("Q2: Sessions of user achille")	
		final.foreach(lambda x: show("	+ " + x[0] + ": " + str(x[1])))   

	
	if qno == '3':
		
		regex='.*?(?:[a-z][a-z]+).*?((?:[a-z][a-z]+)).*?(?:[a-z][a-z]+).*?(?:[a-z][a-z]+).*?(?:[a-z][a-z]+).*?(?:[a-z][a-z]+).*?(?:[a-z][a-z]+).*?((?:[a-z][a-z]+))'
		rg = re.compile(regex,re.IGNORECASE|re.DOTALL)
		ls = sc.textFile(hostDir1 + ',' + hostDir2)
		maps = ls.map(lambda x: None if rg.match(x) is None else (rg.match(x).group(1), rg.match(x).group(2)))
		
		m = maps.filter(lambda x: x is not None and x[0] in hosts).distinct()
		#maps.foreach(lambda x:show(x))
		final= m.reduceByKey(lambda x,y: x + ',' + y)
		show("* Q3: Unique user names")	
		final.foreach(lambda x: show("	+ " + x[0] +": ["+ x[1] + "]"))
	
	if qno == '4':

		regex='.*?(?:[a-z][a-z]+).*?((?:[a-z][a-z]+)).*?(?:[a-z][a-z]+).*?(?:[a-z][a-z]+).*?(?:[a-z][a-z]+).*?(?:[a-z][a-z]+).*?(?:[a-z][a-z]+).*?((?:[a-z][a-z]+))'
		rg = re.compile(regex,re.IGNORECASE|re.DOTALL)
		ls = sc.textFile(hostDir1 + ',' + hostDir2)
		maps = ls.map(lambda x: None if rg.match(x) is None else (rg.match(x).group(1), rg.match(x).group(2)))
		
		m = maps.filter(lambda x: x is not None and x[0] in hosts)


		pairs = m.map(lambda x: ((x[0],x[1]), 1))
		reducedpairs = pairs.reduceByKey(lambda x, y: x + y)
		r = reducedpairs.map(lambda x: (x[0][0], "(" + x[0][1] + "," +  str(x[1]) + ")")) 
		mr = r.reduceByKey(lambda x,y: x + ',' + y)
		show("* Q4: sessions per user")
		mr.foreach(lambda x: show("	+ " + x[0] +": ["+str(x[1]) + "]")) 




	
if __name__ == "__main__":
   main() 
