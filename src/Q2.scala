def calculateCosineDistance(vector1: Array[Double], vector2: Array[Double]): Double = {
	var arr1Sum = 0.0
	var arr2Sum = 0.0
	var numerator = 0.0
	for(i <- 0 until vector1.length){
		arr1Sum += (vector1(i) * vector1(i))
		arr2Sum += (vector2(i) * vector2(i))
		numerator += (vector1(i)*vector2(i))
	}
	return Math.abs(numerator/((Math.sqrt(arr1Sum))*(Math.sqrt(arr2Sum))))
}
def pearsonsCorrCoefficient(x: Array[Double], y:Array[Double]):Double={
	var sumx = 0.0
	var sumy = 0.0
	var xNos = 0;
	var yNos = 0;
	var n = x.length

	for(i <- 0 until n){
		if(x(i) != 0.0){
			xNos = xNos + 1
			sumx += x(i)
		}
		if(y(i) != 0.0){
			yNos = yNos + 1
			sumy += y(i)
		}

	}
	var xmean = sumx/xNos
	var ymean = sumy/yNos

	for(i <- 0 until n){
		if(x(i) != 0.0){
			x(i) = x(i) - xmean	
		}
		if(y(i) != 0.0){
			y(i) = y(i) - ymean	
		}
	}

	val distance = calculateCosineDistance(x,y)
	return distance
}
val data = sc.textFile("C:\\Users\\Avinash\\Downloads\\hw4Important_Materials_dataset\\itemusermat").map(t => (t.split(" ")(0), (t.substring(t.indexOf(" ")).trim().split(' ').map(_.toDouble))))
var movieID = readLine("Enter Movie ID: ")

val movieMap = data.collectAsMap()
val imdb = sc.textFile("C:\\Users\\Avinash\\Downloads\\hw4Important_Materials_dataset\\movies.dat").map{
			t => (t.split("::")(0),(t.split("::")(1),t.split("::")(2)))
			}.collectAsMap()
var rating = movieMap(movieID)
var movieSimilarity = movieMap.map{ movie =>
		val similarity = pearsonsCorrCoefficient(rating,movie._2)
		(movie._1,similarity)
}.toSeq.sortBy(-_._2)
println()
for(i <- 0 until 6){
	println(movieSimilarity(i)._1 + " "+ imdb(movieSimilarity(i)._1))
}