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

### Per inserire il dato covid (true/false)

db.videos.update(
    {query (stile find) con espressione regolare}, #estrae i documenti da modificare
    {$set : {covid : true},
    {multi : true} #fa in modo di modificare tutti i documenti trovati
)

_Nota_: in teoria non è necessario negli altri documenti settare covid a false però forse è più sicuro farlo, 
per settare tutti i documenti con covid false:

db.videos.update(
    {},
    {$set : {covid : false},
    {multi : true}
)