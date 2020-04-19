# Query mongo per estrarre informazioni interessanti

### Contare quanti video hanno tag coronavirus per paese:

db.videos_march.aggregate(
    [{$match : {
                tags : {$all : ["coronavirus"]}}},
     {$group : {
                _id : "$country_name", 
                count : {$sum : 1}}
     }
    ]
)

Result (dati fino 16.04.20):

{ "_id" : "Messico", "count" : 1742 }

{ "_id" : "Canada", "count" : 3242 }

{ "_id" : "Regno Unito", "count" : 2811 }

{ "_id" : "Corea del sud", "count" : 39 }

{ "_id" : "India", "count" : 2018 }

{ "_id" : "Francia", "count" : 2717 }

{ "_id" : "USA", "count" : 3236 }

{ "_id" : "Russia", "count" : 122 }

{ "_id" : "Italia", "count" : 1591 }

{ "_id" : "Germania", "count" : 1779 }

{ "_id" : "Brasile", "count" : 695 }
