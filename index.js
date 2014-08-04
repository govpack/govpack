#!/usr/bin/env node
process.on('uncaughtException',function(er){console.log(er.stack)})
var fs=require('fs');var LOG=console.log
//function LOG(){console.log.apply(null,arguments)}
var http=require('http');http.globalAgent.maxSockets=500
var https=require('https');https.globalAgent.maxSockets=500
var cp=require('child_process');
var DIR=(__dirname+'/').replace(/\x5c/g,'/');
var DAT=DIR+'govdat/'
var ha=__dirname.split(/[\\\/]/).slice(0,2).join('/')+'/';

var j={k:function(b,places){/*Bytes to FileSize.places[b|K|MB|GB] string*/var x='b';if(b>1024){b=b/1024;if(b<1024){x='K'}else{b=b/1024;if(b<1024){x='MB'}else{b=b/1024;x='GB'}}}return b.toFixed(isNaN(places)?1:places)+''+x}}
var l={j:function(o,s,q){/*JSON.stringify alternative*/if(s==2){return JSON.stringify(o)}if(o===null){return 'null'};function DS(s){return s.replace(/[\\]/g, '\\\\').replace(/'/g, '\\\'').replace(/[\b]/g,'\\b').replace(/[\f]/g,'\\f').replace(/[\n]/g,'\\n').replace(/[\r]/g,'\\r').replace(/[\t]/g,'\\t')};switch(typeof o){case 'undefined':return 'undefined';case 'string':return '\''+DS(o)+'\'';case 'number':return ''+o;case 'object':if(o.constructor.toString().indexOf('Date()')>0){return o.getTime()}if(o.constructor.toString().indexOf('RegExp()')>0){return ''+o}var RA=[];var AR=(o.constructor.toString().indexOf('Array()')>0);var v;for(var p in o){if(!o.hasOwnProperty(p)){continue}if(s){v=''+o[p]}else{v=this.j(o[p])}if(AR){RA.push(v);continue}if(q){RA.push('"'+p+'":'+v);continue}if((/^[$\w]+$/.test(p) && !/^\d/.test(p))||/^\d+$/.test(p)){RA.push(p+':'+v);continue}RA.push('"'+DS(p)+'":'+v)}return ((AR?'[':'{')+RA.join(',')+(!AR?'}':']')).replace(/},/g,'}\n,').replace(/,T:\[/g,',\n   T:[').replace(/,C:\[/g,',\n   C:[').replace(/,R:\{/g,',\n   R:{');default:return ''+o}}
,E:function (fp){/*Open file in TextEditor*/cp.exec(TextEditor+' '+fp.replace(/file:\x2f+/,'').replace(/[?#][^/]+$/,''),{},function(er/*,stdout,stderr*/){if(er){LOG('######### run.js.l.E()###### '+er.stack)}})}
,md:function(fp){/*MakeDirectory: Check or make a folder/folders. Input a path with forward slashes only, ending in "/". Returns the path or an empty string if it fails.*/fp=''+fp;var fp0=fp.replace(/\x5c/g,'/');try{if(fs.statSync(fp0).isDirectory()){return fp0}}catch(er){}var p=fp.indexOf('\\');var q=fp.indexOf('/',Math.max(3,p));var fp1=fp.substr(0,q).replace(/\x5c/g,'/');if(!fp1){return ''}var fp2=fp1+'\\'+fp.substr(Math.max(p,q)+1);var fp3=fp1.replace(/\x5c/g,'/');try{if(fs.statSync(fp3).isDirectory()){return this.md(fp2)}}catch(er){}try{fs.mkdirSync(fp3,process.umask())}catch(er){return ''}return this.md(fp2)}
}

if(!l.md(DAT)){LOG('Failed to find or make folder:'+DAT);process.exit()}

var TextEditor=ha+'C/np/notepad++.exe'
var DIS=DAT+'CSV/' /*Changed according to {format:'XYZ'}*/
var FORMAT='csv';var FOM='CSV';var fom='csv' 
var OK=true
var ZAM=DIR+'ufo/aa.txt'
var o4=ObjectFromJsonpFile(ZAM)
var msg=''

if(typeof o4=='object'){
if(!o4.CK){OK=false;msg+='\n with property CK'}else{
if(!o4.CK.pop){OK=false;msg+='\n CK needs to be an Array CK:[]'}
if(!o4.CK.length){OK=false;msg+='\n Array CK[] needs some entries CK:[{},{}]'}
}}else{OK=false;msg+='\n it needs to be an object'}
if(OK===false){LOG('FIX '+ZAM+'\nand retry'+msg);LOG(o4);process.exit()}
CK=o4.CK

var o1={MaxPages:10,PageSize:10,format:'csv'} 
var o3={}

if(require.main==module){/**A*Use from the CommandLine: RUN "node.exe index.js {fetch:0|1|2}" OR index.js {filter:1} etc*/ 
var ar=process.argv.slice(2).join(' ').replace(/^["']+|["']+$/g, '')
try{eval('var o2=('+ar+')')}catch(er){LOG(er.message);OK=false}
if(typeof o2=='object'){for(var p in o2){o3[p]=o2[p]}}
LOG('govpack is seeking out '+FOM+' dataset metadata')
LOG('from some number X=0|1|2 of CKAN API endpoints (package_list_with_resources)\n Usage:') 
LOG('govpack {fetch:X} --> makes X.js module.exports=BigPackageList') 
LOG('govpack {filter:X} --> makes X.txt filtered JSONP IIII(filtered_csv_metadata)') 
LOG('govpack {download:X} --> downloads ./CSV/1.csv, ./CSV/2.csv,,, ./CSV/n.csv ')
LOG('downloaded '+DAT+'/format/1...n.format files match up with the metadata in X.txt')
LOG('Use from the Command Line or as one of your npm node_modules')
LOG('Look for the results in this folder: (maybe: node_modules/govpack/index.js)\n'+DAT)
LOG('Optionally specify a filetype {format:\'XYZ\'} in the filter step') 
LOG('govpack {filter:1, format:\'xlsx|ckml|rdf|odp|dat|etc\'}')
LOG('the default filter format is CSV')
LOG('govpack {filter:1, format:\'csv\'}')
if(OK){init(o3)}else{LOG('Fix json command line argument and retry');process.exit()}
}else{/**A*Use as a module:GP=require('govpack');GP({fetch:1,filter:1,format:'xls'},cb)*/module.exports=init}

var VX='api/3/action/'

function KK(x){var o=CK[x];if(!o){LOG(-11);return ''}
var fp=o.U;if(!fp){LOG(-22);return ''}
if(typeof fp!='string'){LOG(-33);return ''}
return fp}

function PK(x){var o=CK[x];if(!o){LOG(-1);return ''}
if(o.V){return o.V /*allows setting full endpoint uri from the ufo/aa.txt config*/}
var fp=o.U;if(!fp){LOG(-2);return ''}
if(typeof fp!='string'){LOG(-3);return ''}
return fp+VX}

function init(o,cb){/*The one and only function exported and required. Calls internal functions based on settings in o*/
if(typeof o.port=='number'){return Serve(o,cb)}
for(var p in o1){/*add defaults from o1*/if(!o.hasOwnProperty(p)){o[p]=o1[p]}}
cb=cb||function(){};
var x=parseInt(o.download)
if(isNaN(x)){x=parseInt(o.fetch)}
if(isNaN(x)){x=parseInt(o.filter)}
if(isNaN(x)){x=parseInt(o.f)}
o.x=x;o.u=KK(x)
if(!o.u){return LOG('We dont have CKAN API #'+x+' "'+PK(x)+'" URL please add it. Or use another number and re-try.')}
if(o.silent){LOG=function(){} /*turn terminal chatter off*/}
if(o.format){FORMAT=o.format.toString().toLowerCase()}
FOM=FORMAT.toUpperCase();fom=FOM.toLowerCase()
DIS=DAT+FOM+'/' 
if(typeof o.filter=='number'){LOG('Now we will filter and refine the Big DataSet List @'+(DAT+x+'.js')+'\n~(hopefully that\'s in place OR RUN govpack {fetch:'+o.filter+'} to put it there)\nFiltering for on datasets/resources where format='+FOM+'\nAnd using a datastore_search query on API#'+o.filter+'\n'+CK[o.filter].url+'api/action/datastore_search?resource_id=###\nto get the size, description, field names, data types, row count, and the first row of actual data!!');ScanList(o,cb);return}
o.EP=['api/rest/dataset/','api/2/rest/dataset/','api/3/action/']
LOG(o.EP)
CheckEndpoints(o,cb)
}

function CheckEndpoints(o,cb){/*Check for a 200 http response from one of the endpoints before proceeding*/
if(!o.EP.length){return LOG('Tried all 3 no response from those endpoints')}VX=o.EP.pop();o.vx=VX
LOG('Checking server response from CKAN above endpoints\n'+VX+'')
var url=o.u+VX;
LOG(url)
var ck=CK[o.x];if(ck){if(ck.V){/*1go*/url=ck.V}}

var r=url.indexOf('https')?require('http'):require('https')
r.get(url,function(R){LOG('statusCode:'+R.statusCode)
if(R.statusCode==200||R.statusCode==301||R.statusCode==404){LOG(url+' seems ok, proceeding with "'+VX+'"...');NowProceed(o,cb)}else{return CheckEndpoints(o,cb)}
R.on('data',function(c){LOG(c)});
R.on('end',function(){LOG(url+' finished responding')})
}).on('error',function(er){LOG(url+' gave an error:');LOG(er);return CheckEndpoints(o,cb)})

}

function NowProceed(o,cb){/*One of the ckan endpoint returned "200" so proceed*/var x=o.x
if(!fs.existsSync(DIS)){try{fs.mkdirSync(DIS)}catch(er){LOG('Failed to make '+DIS);return cb(er,{})}}
if(!fs.existsSync(DIS)){LOG('Failed to find or make '+DIS);return cb({bad:1,d:'Failed to find or make'+DIS},{})}
var fp1=DAT+x+'.js' /*module.exports={BigList}*/
var fp2=DAT+x+'.txt' /*Our refined list of IIII(jsonp)*/
if(typeof o.f=='number'){LOG('Please be patient while we fetch AND filter from API#'+x);GetBiggerList(o,(function(o,cb){return function(){ScanList(o,cb)}}(o,cb)) );return}
if(typeof o.download=='number'){LOG('Given {fetch:'+x+'} and {filter:'+x+'} MADE an EXISTING local vaild JSONP list at '+fp2+'\n\tcheck it out.The above file should be good for use inside any Browser based App\n\tCross Domain, Mobile or Desktop, Online or Offline, via file: OR http: protocols\nWe will now proceed to download '+FOM+' resources from online.\nResources '+DAT+FOM+'/1,2,3,4.'+FOM.toLowerCase()+', etc will be saved to match the numeric Array index in\n<script src="'+fp2+'"></script>\nAs surfaced on an html page via a JSONP reciever function IIII(resource_list ){/*Got resource_list */}\nAfter downloading we should have the metadata and the DATA!!');DownloadMany(o,cb);return}
if(typeof o.fetch=='number'){LOG('Please be patient while we fetch from CKAN v3 API#'+o.fetch);GetBiggerList(o,cb);return}
}

function Serve(o){/*A*Use via http. Usage:  
var xhr=new XMLHttpRequest();//works from a webpage calling to localhost port 80:
xhr.onload=function(){prompt(this.status,this.responseText)}
xhr.open('POST','http://127.0.0.1:80/',true);
xhr.send(JSON.stringify({fetch:55,etc:''}));*/
var port=80;if(o.port!=1 && typeof port=='number'){port=~~o.port}
LOG('govpack {fetch:X} server listening at http://127.0.0.1:'+port+'/');
var server=http.createServer(HTTP).listen(port);server.on('error',function(er){LOG(er.message)})
}

function HTTP(r,R){/*Call init(o) if we recieve a json object from an xhr post*/var js='';
R.setHeader('Access-Control-Allow-Origin', '*');// Website you wish to allow to connect
R.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, PUT, PATCH, DELETE');// Request methods you wish to allow
R.setHeader('Access-Control-Allow-Headers', 'X-Requested-With,content-type');// Request headers you wish to allow
R.setHeader('Access-Control-Allow-Credentials', true); // Set to true if you need the website to include cookies in the requests sent
r.on('data',function(s){js+=s});
r.on('end',function(){var o=false;
try{/*eval('var o=('+js+')')*/o=JSON.parse(js)}catch(er){return R.end(JSON.stringify(er))}
if(typeof o!='object'){return R.end(JSON.stringify({bad:1,d:'Fix json POST and retry'}))}
init(o,function(er,o){R.end(JSON.stringify(er||o))})
})
}


function ObjectFromJsonpFile(fp){/*Get object from a DATA(jsonp) file.*/
try{var JS=fs.readFileSync(fp).toString().replace(/*\uFEFF=BOM*//^\uFEFF/,'');
JS=JS.substring(JS.indexOf('('),JS.lastIndexOf(')')+1)}catch(er){LOG(er.stack);return 0}
var o=0;try{eval('var o='+JS)}catch(er){LOG(er.stack);return 0}
if(typeof o!='object'){return 0}return o}

function DownloadMany(o,cb){var x=o.download;DIS=DAT+x+'/'+o.format+'/'
fom=o.format.toLowerCase();FOM=o.format.toUpperCase()
if(!l.md(DIS)){var oh='Download function failed to find or make folder:'+DAT;LOG(oh);o.bad=1;return cb({bad:1,message:oh},o)}
var fp1=DAT+x+'.js' /*module.exports={BigList}*/
var fp2=DAT+x+'.txt' /*Download from data inside IIII(resource_array)*/

var JS='';var SIZE=0;var A=ObjectFromJsonpFile(fp2)
if(A==0){o.d='DownloadMany failed to extract an ARRAY/LIST/OBJECT';LOG(o.d);o.bad=1;return cb({bad:1,message:o.d},o)}
if(!A.pop){o.d='DownloadMany failed to extract an ARRAY/LIST';LOG(o.d);o.bad=1;return cb({bad:1,message:o.d},o)}
LOG('Downloading and Saving to...\n '+DIS)
DoNext()

function DoNext(er,size){if(typeof size=='number'){SIZE+=size}
var o=A.pop();if(!A.length){/*A[1] should be the last, A[0] has title info only*/return Done(cb)}
if(!o){return DoNext()}
if(!o.url){return DoNext()};
var URL=o.url
var fn=A.length
var FP=DIS+fn+'.'+fom
LOG('#'+fn+'. '+o.url)
LOG('#'+fn+'. '+FP)
DownloadOne(URL,FP,DoNext)
}

function Done(cb){LOG('Finished downloading '+FOM+' files.\nCheck folder for results.\n'+DIS);
cb(null,{d:'Finished downloading '+FOM+' files.\nCheck folder for results.\n'+DIS,size:SIZE})}

}

function DownloadOne(URL,FP,cb){var file=fs.createWriteStream(FP)
var web=(URL.charAt(4)=='s'?https:http);
web.get(URL,function(R){R.pipe(file);file.on('finish',function(){file.close();
fs.stat(FP,function(er,me){if(er){LOG(er.stack);return cb(er)}LOG('\t\t['+j.k(me.size)+'] now in '+FP+' from '+URL+'\n\n');cb(null,me.size)})
})})}

function GetBiggerList(o,cb){var x=o.x;cb=cb||function(){}
/*Bigger current_package_list_with_resources won't come down all at once
so the idea here is to request it in parts: "?limit=100&page=1,2,3,4"  
and then objectify and merge the parts and save that lot as as one file*/
var MaxPages=o.MaxPages||3;var PageSize=o.PageSize||10;var TXT='';
var URL=PK(x)+'current_package_list_with_resources?limit='+PageSize+'&page='
var web=(URL.charAt(4)=='s'?https:http)
var OBJ={help:'...',success:true,result:[]}
var RZA=OBJ.result
var N=0;var FP=DAT+x+'.js'
var EP='['+URL.split('/')[2]+ '] ('+PageSize+'/PerPage)'
LOG('Requesting package_list_with_resources pages:'+(N+1)+' to '+MaxPages+' ['+PageSize+' records per page] \nWill Save As: '+FP+'\n')

PaginNate()

function PaginNate(){N+=1;var getMore=(N<=MaxPages);if(getMore){
LOG((parseInt((N-1)/MaxPages*100))+'% Got '+(N-1)+'/'+MaxPages+' pages - '+(MaxPages-N+1)+' more from '+EP)
setTimeout(function(){GetPage(N)},40)
}else{LOG('Pagination Complete');DONE()}}

function GetPage(n){var url=URL+n;
web.get(url,function(R){var txt='';
R.on('data',function(t){txt+=t})
R.on('end',function(){MergeThem(null,txt)}
)})}

function MergeThem(er,js){if(er){return PaginNate()}var o=0;
try{eval('var o=('+js+')')}catch(er){TXT+=er.message;LOG(er.stack);o=0}
if(typeof o=='object'){delete o.help;
if(!o.success){return PaginNate()}
var R=o.result;if(typeof o!=='object'){return PaginNate()}
if(!R.pop){return PaginNate()}var ob=null;
for(var i=0;i<R.length;i++){ob=R[i];
/*could also filter here on resources[].format*/
if(ob.description){LOG(ob.description)}
RZA.push(ob)}
}
LOG('\t\t    - Added '+R.length+' records ['+RZA.length+'total]')
PaginNate()
}

function DONE(){var txt='module.exports='+JSON.stringify(OBJ)
fs.writeFile(FP,txt,{},function(er){if(er){LOG(er.stack);return cb(er)} 
LOG('GetBiggerList('+x+') has finished!!');return cb(null,{d:'done'});})}
}


function ScanList(o,cb){var x=o.x;cb=cb||function(){}
LOG(x)
LOG(o)
fom=o.format.toLowerCase();FOM=o.format.toUpperCase()
var C=[{DataSets:0,Fields:0,CKAN:x,format:fom,made:(new Date()).toLocaleString(),ms:Date.now()}];
var DataSets=0;var Fields=0;/*CountThem*/
var fp1=DAT+x+'.js' /*module.exports={BigList}*/
var fp2=DAT+x+'.txt' /*IIII([{},{},{}]) our refined list of CSV datasets,sizes, row count, field Names, field types, and the first row of sample data*/
var B=[];var JS=''
var O=require(fp1)
if(!O){LOG(-4);return cb({message:'Failed to find package list on disc. Run govpack.cmd {fetch:'+x+'} and retry'})}
var A=O.result
if(!A){LOG(-5);return cb({message:'Failed to objectify package list from file. Run govpack.cmd {fetch:'+x+'} and retry'})}
if(!A.pop){LOG(-6);return cb({message:'Failed to arrayify package list from above. Run govpack.cmd {fetch:'+x+'} and retry'})}
LOG('\n\tFOUND ['+A.length+'] packages/datasets\n\twith many subfiles and linked resources...')
LOG('  ...now scanning to find the titles, field-names\n\t\tand row count for each ['+FORMAT.toUpperCase()+'] resource....\n\t\t\t\t......\n')
var fe=null;
var LEN=A.length;var o=null;

function Done(cb){
C[0].DataSets=DataSets;C[0].Fields=Fields
try{fs.writeFileSync(fp2,'IIII('+l.j(C)+')','utf8')}catch(er){LOG(-1);LOG(er.message);LOG(er.stack);return cb(er,{d:'Failed to Save',fp:fp2})}
cb(null,{Saved:fp2})
}

var UP2=-1 /*some way to say that we may be done, since the C array expands out on multiple resources*/
DoNext()
function DoNext(){LOG(' A:'+A.length+ '  C:'+C.length+' H:'+UP2+' '+(C.length==UP2)?' =====':'|||||')
var o=A.pop();
if(A.length==0 && C.length==UP2){LOG('..............');return Done(cb)}
if(C.length>UP2){UP2=C.length}
if(!o){return DoNext()}
if(!o.resources){return DoNext()};B=o.resources;
if(!B){return DoNext()}
if(!B.length){return DoNext()}
for(n=0;n<B.length;n++){bb=B[n] ;if(!bb){return DoNext()};if(!bb.format){return DoNext()}
if(typeof bb.format!='string'){return DoNext()}
LOG(bb.format) /**bb.mimetype may also be useful here 
also may consider adding regex match, since we have seen bb.format='(excel) xls' **/
if(bb.format.toLowerCase()!=fom){return DoNext()}
//if(bb.format.toLowerCase().indexOf(FORMAT)==-1){return DoNext()}

if(!bb.url){return DoNext()}
ux=PK(x)+'datastore_search?resource_id='+bb.id+'&limit=1'

var web=(ux.charAt(4)=='s'?https:http)
var oo={rows:0/*fe.result.total*/,cols:0/*fe.result.fields.length*/
,size:parseInt(bb.size)||-1
,d:bb.title||bb.description||''
,url:bb.url,id:'fe.result.resource_id',made:bb.created
,mods:bb.last_modified,C:[],T:[],R:0}


web.get(ux,function(R){var js='';
R.on('error',function(){return setTimeout(DoNext,0)})
R.on('data',function(t){js+=t;})
R.on('end',function(){js+='';
try{var fe=JSON.parse(js)}catch(er){LOG(js)+'#######'+er.message+'####NaughtyJSON#######';return setTimeout(DoNext,0)}

if(!fe){return DoNext()}
if(typeof fe!='object'){return DoNext()}
if(!fe.result){return DoNext()}
if(!fe.result.fields){return DoNext()}
oo.id=fe.result.resource_id
oo.rows=fe.result.total
if(fe.result.records){
if(fe.result.records[0]){oo.R=fe.result.records[0]}
}
var FZ=fe.result.fields
if(FZ){
if(FZ.length){oo.cols=FZ.length
for(var i=0;i<FZ.length;i++){Fields+=1;
oo.C[i]=FZ[i].id
oo.T[i]=FZ[i].type
}
}
}

C.push(oo);DataSets+=1
LOG('['+DataSets+' DataSets]   ['+Fields+' Fields]')
setTimeout(DoNext,0)
})})

}}}




//process.stdin.resume()
