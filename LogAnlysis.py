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

	
	if qno == '5':

		regex ='.*?(?:[a-z][a-z]+).*?((?:[a-z][a-z]+))([\s\S]+[\w\W]+[\d\D])'
		rg = re.compile(regex,re.IGNORECASE|re.DOTALL)
		ls = sc.textFile(hostDir1 + ',' + hostDir2)
		maps = ls.map(lambda x: None if rg.match(x) is None else (rg.match(x).group(1), rg.match(x).group(2)))
		
		m = maps.filter(lambda x: x is not None and x[0] in hosts  and 'error' in x[1])

		pairs = m.map(lambda x: (x[0], 1))
		r =pairs.reduceByKey(lambda x,y: x+y).sortByKey()
		show("* Q5: number of errors")
		r.foreach(lambda x: show("	+ " + x[0] + ": " + str(x[1])))  


	if qno == '6':

		regex ='.*?(?:[a-z][a-z]+).*?((?:[a-z][a-z]+))([\s\S]+[\w\W]+[\d\D])'	
		rg = re.compile(regex,re.IGNORECASE|re.DOTALL)
		ls = sc.textFile(hostDir1 + ',' + hostDir2)
		maps = ls.map(lambda x: None if rg.match(x) is None else (rg.match(x).group(1), rg.match(x).group(2)))
		
		m = maps.filter(lambda x: x is not None and x[0] in hosts  and 'error' in x[1])

		pairs = m.map(lambda x: ((x[0], x[1]), 1))
		pairsreduced=pairs.reduceByKey(lambda x,y: x+y)
		

		r = pairsreduced.map(lambda x: (x[0][0],  (x[0][1], x[1]))).groupByKey().mapValues(list)
		
	
		show("* Q6: 5 most frequent error messages")
		r.foreach(lambda x: Q6(x[0], x[1]))

	if qno == '7':

		regex='.*?(?:[a-z][a-z]+).*?((?:[a-z][a-z]+)).*?(?:[a-z][a-z]+).*?(?:[a-z][a-z]+).*?(?:[a-z][a-z]+).*?(?:[a-z][a-z]+).*?(?:[a-z][a-z]+).*?((?:[a-z][a-z]+))'
		rg = re.compile(regex,re.IGNORECASE|re.DOTALL)
		ls = sc.textFile(hostDir1 + ',' + hostDir2)
		maps = ls.map(lambda x: None if rg.match(x) is None else (rg.match(x).group(2), rg.match(x).group(1)))
		
		m = maps.filter(lambda x: x is not None and x[1] in hosts).distinct()

		pairs = m.map(lambda x: (x[0], 1))
		pairsReduced = pairs.reduceByKey(lambda x,y: x + y)

		f = pairsReduced.filter(lambda x: x[1] == 2)
		show("Q7: users who started a session on both hosts, i.e., on exactly 2 hosts.")	
		f.foreach(lambda x: show("	+ : "+ x[0])) 

	if qno == '8':

		regex='.*?(?:[a-z][a-z]+).*?((?:[a-z][a-z]+)).*?(?:[a-z][a-z]+).*?(?:[a-z][a-z]+).*?(?:[a-z][a-z]+).*?(?:[a-z][a-z]+).*?(?:[a-z][a-z]+).*?((?:[a-z][a-z]+))'
		rg = re.compile(regex,re.IGNORECASE|re.DOTALL)
		ls = sc.textFile(hostDir1 + ',' + hostDir2)
		maps = ls.map(lambda x: None if rg.match(x) is None else (rg.match(x).group(2), rg.match(x).group(1)))
		
		m = maps.filter(lambda x: x is not None and x[1] in hosts).distinct()

		pairs = m.map(lambda x: (x[0], (x[1],1)))
	
		pairsReduced = pairs.reduceByKey(lambda x,y: (x[0], x[1]+ y[1]))

		r = pairsReduced.filter(lambda x: x[1][1] == 1)
		show("* Q8: users who started a session on exactly one host, with host name.")	
		r.foreach(lambda x: show("	+ " + str(x[0])+": "+str(x[1][0]))) 



	
if __name__ == "__main__":
   main() 
