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
    {tags : {$in : [regex]}}, #estrae i documenti da modificare
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


### Regular expression Covid (javascript)
/(corona|covid|virus|pandemi[aec]|epidemi[aec]|tampon[ei]|sierologico|mascherin[ae]|코로나 바이러스|fase\s*(2|due)|iorestoacasa|stayathome|lockdown|[qc]uar[ae]nt[ei]n[ea]|कोरोनावाइरस|ਕੋਰੋਨਾਵਾਇਰਸ|massisolation|distanziamento\s*sociale|social\s*distancing|감염병 세계적 유행|パンデミック|コロナウイルス|सर्वव्यापी महामारी|ਸਰਬਵਿਆਪੀ ਮਹਾਂਮਾਰੀ|пандемия|коронавирус|social\s*distancing|distanciamiento\s*social|코로나|कोविड|ਕੋਵਿਡ)/i


### merge mongoDB
db.videos_march.aggregate(
    [
        {
            $match : {}
        },
        {
            $lookup:
            {
                from: "covid",
                localField: ["trending_date", "country_name"],
                foreignField: ["date", "location"],
                as: "video_prova"
            }
        }
    ]
)

db.videos_march.aggregate([
{
    $lookup:
        {
          from: "covid",
          localField: "trending_date",
          foreignField: "date",
          as: "merge1"
        }
},  
{$unwind :"$merge1" },
{ 
     $project: { 
            mid: { $cond: [ { $eq: [ '$location', '$merge1.country_name' ] }, 1, 0 ] }, 
            date : "$merge1.country_name"
        } 
},
{$match : { mid : 1}}

])