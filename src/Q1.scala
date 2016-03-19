class movie(val movieid: String, val dist: Double, val ratings: Array[Double]) extends java.io.Serializable{
	var movieID: String = movieid
	var distance: Double = dist
	var movieRatings: Array[Double] = ratings
	def getMovieID():String = {
		return movieID
	}
	def getDistance():Double = {
		return distance
	}
	def getRatings():Array[Double] = {
		return movieRatings
	}
}
def calculateCosineDistance(vector1: Array[Double], vector2: Array[Double]): Double = {
	var arr1Sum = 0.0
	var arr2Sum = 0.0
	var numerator = 0.0
	for(i <- 0 until vector1.length){
		arr1Sum += (vector1(i) * vector1(i))
		arr2Sum += (vector2(i) * vector2(i))
		numerator += (vector1(i)*vector2(i))
	}
	return (1-(numerator/((Math.sqrt(arr1Sum))*(Math.sqrt(arr2Sum)))))
}
def closestPoint(p: Array[Double], centroids: Array[(String,Array[Double])]): String = {
	var index = 0
	var movieId = 0
	var closest = Double.PositiveInfinity
	for (i <- 0 until centroids.length) {
		var movieEntry = centroids(i)
		var rating = movieEntry._2
		val distance = calculateCosineDistance(p,rating)
		if(distance < closest){
			index = i
			closest = distance
		}
	}
	return index + " " + closest
}
def findNewClusteroid(clusterHead: String, headRatings: Array[Double], movies: Iterable[movie]): movie ={
	var noOfItems = 0
	var clusterDistance = 0.0
	for(item <- movies){
		clusterDistance = clusterDistance + calculateCosineDistance(headRatings,item.getRatings())
		noOfItems = noOfItems + 1
	}
	var minAvgDistance = clusterDistance/noOfItems
	var clusterHeadMovie = clusterHead
	var clusterHeadRatings = headRatings
	for(movie <- movies){
		var tmpDistance = 0.0
		var ratings = movie.getRatings()
		for(innerMovie <- movies){
			tmpDistance = tmpDistance + calculateCosineDistance(ratings,innerMovie.getRatings())
		}
		var dist = tmpDistance/noOfItems
		if(dist < minAvgDistance){
			clusterDistance = tmpDistance
			clusterHeadMovie = movie.getMovieID()
			clusterHeadRatings = movie.getRatings()
		}
	}
	return new movie(clusterHeadMovie,clusterDistance,clusterHeadRatings)
}
val data = sc.textFile("C:\\Users\\Avinash\\Downloads\\hw4Important_Materials_dataset\\itemusermat").map(t => (t.split(" ")(0), (t.substring(t.indexOf(" ")).trim().split(' ').map(_.toDouble)))).cache()
val count = data.count()
val K = 10
println("Number of records " + count)
var centroids = data.takeSample(false, K, 32)
var diff = 0
var fiveMovies = new Array[(String, Array[String])](10)
do {
	var centroidMap = Set[String]()
	for(i <- 0 until centroids.length){
		centroidMap += centroids(i)._1
	}
    var closest = data.map{p => 
		val clusterDistance = closestPoint(p._2, centroids).split(" ")
		(clusterDistance(0).toInt, (new movie(p._1,clusterDistance(1).toDouble,p._2)))
	}
    var moviesGroup = closest.groupByKey()
	var newCentroids = moviesGroup.map{cluster => 
		val movieId = centroids(cluster._1.toInt)._1
		val rating = centroids(cluster._1.toInt)._2
		var newClusterHead = findNewClusteroid(movieId,rating,cluster._2)
		(newClusterHead.getMovieID(),newClusterHead.getRatings())
	}.collect()
    diff = 0
    for(i <- 0 until newCentroids.length){
		if(!centroidMap.contains(newCentroids(i)._1)){
			diff = diff + 1
		}
	}
    println(diff)
	centroids = newCentroids
	
	if(diff == 0){
		fiveMovies = moviesGroup.map{cluster =>
				val movieID = centroids(cluster._1.toInt)._1
				
				var count = 0;
				var movies = new Array[String](5)
				var itr = cluster._2.take(5)
				for(item <- itr){
					movies(count) = item.getMovieID()
					count = count + 1
				}
				(movieID,movies)
		}.collect()
	}
	
} while (diff != 0)
println("Done")
val imdb = sc.textFile("C:\\Users\\Avinash\\Downloads\\hw4Important_Materials_dataset\\movies.dat").map{
			t => (t.split("::")(0),(t.split("::")(1),t.split("::")(2)))
			}.collectAsMap()
var count = 1
fiveMovies.foreach{cluster =>
	println("Cluster "+count+":")
    var movie = cluster._1
    var movies = cluster._2
    println("Cluster head "+movie+ " "+imdb(movie))
    println()
    for(i <- 0 until movies.length){
		if(movies(i)!= null){
			print(movies(i)+" "+imdb(movies(i))+" ")
		}
		println()
	}
	println("---------------------------------------------------")
	count = count+1
}