extern crate core;

use std::borrow::{Borrow, BorrowMut};
use std::cmp::Ordering;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::f64::NEG_INFINITY;
use std::fs::{File, OpenOptions};
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::sync::{Mutex, Arc};
use std::{io};
use std::io::Write;
use std::os::unix::raw::nlink_t;
use std::process;
use std::time::{SystemTime, Duration};
use chrono::NaiveWeek;
use mongodb::IndexModel;
use mongodb::options::{IndexOptions, AggregateOptions};
use mongodb::{Client, options::{ClientOptions, ResolverConfig}, bson::doc, Collection};
use bson::Document;
use futures::stream::TryStreamExt;
// use tokio::stream::StreamExt;
use csv::{ReaderBuilder, Writer};
use fp_growth::algorithm::FPGrowth;
use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use ::esx_mining::rule::*;
use anyhow::{Context, Result};
use toml::*;
use clustering::*;
use rand::random;
use gsdmm::*;
use clap::{Parser, ValueEnum, Arg};

type mutex_u64 = Arc<Mutex<u64>>;

#[derive(clap::ValueEnum,Parser, Debug, PartialEq,Clone)]
enum RankingPara {
    EsxRate,
    CScore,
    Qrul,
    LDistance,
    QrulFreq,
}

#[derive(clap::ValueEnum,Parser, Debug, PartialEq,Clone)]
enum JobType {
    RankingParaCompare,
    SupportRateCompare
}




#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
   /// data path
   #[arg(short, long,default_value="./data/azure_data_2000000.csv")]
   data_path: String,
    /// output path
    #[arg(short, long,default_value="result_of_mining.csv")]
    output_path: String,

   /// number of log entries to load
   #[arg(short, long, default_value_t = 2000000)]
   loaded_number: u64,

    /// 
    #[arg(short, long, default_value_t = 1.0)]
    balance_para: f64,

    /// which balance parameter to use
   #[clap(short,long,value_enum,default_value="esx-rate")]
    ranking_para: RankingPara,

    #[clap(short,long,value_enum,default_value="ranking-para-compare")]
    job_type: JobType,

}



#[tokio::main]
async fn main()->Result<()> {
    println!("Start Mining Benchmark!");






    let args = Args::parse();
    println!("{:?}", args);

    println!("DataPath: {}", args.data_path);
    println!("Output Path:{} ", args.output_path);
    println!("ranking parameter: {:?}",args.ranking_para);
    println!("jobType: {:?}",args.job_type);
    println!("balance rate: {}",args.balance_para);
    

    let mut file = OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open("output_scoring_supportRate01.csv").unwrap();
    // let mut wtr = Writer::from_path("output_scoring.csv")?;
    let mut wtr = Writer::from_writer(file);

    // if args.job_type == RankingPara


    // let start_time = SystemTime::now();
    // let mut trans_2:Vec<Vec<String>> = Vec::new();
    // let mut para_2:Vec<Vec<String>> = Vec::new();
    // let mut set2:HashSet<String> = HashSet::new();
    // (trans_2,para_2,set2) = load_csv_by_shard("./data/azure_data.csv", 10000).unwrap();
    // let end_time2 = SystemTime::now().duration_since(start_time).unwrap().as_secs_f64();
    // println!("tran1cmp:{},{} ; para_1cmp:{},{};set_cmp:{},{};time1:{},time2:{}",trans_1.len(),trans_2.len(),set_para.len(),para_2.len(),set1.len(),set2.len(),end_time1,end_time2);
    let loaded_num = 2000000;

    // load_csv_with_para_and_backup_loaded("./data/azure_data.csv", loaded_num, "transaction_backup").await.unwrap();
    
    // let omega = 2.0;
    // for n in 1..5{
    //     let scoring_rate = (41.0-(n*10) as f64) /100.0;
    //     for obp_it in 1..10{
    //         let obp_rate:f64 = obp_it as f64/10.0;
    //         let rule_set = mining_rule_with_fp_growth("./data/data_final.csv",omega.clone(),loaded_num.clone(),scoring_rate.clone(),obp_rate.clone(),0.2).await?;
    //         let (precsion,TPR,FPR ,TPRnd,FPRnd )= evaluation_policy(&rule_set).await?;
    //         wtr.write_record(&[
    //             loaded_num.clone().to_string(),
    //             scoring_rate.clone().to_string(),
    //             omega.clone().to_string(),
    //             obp_rate.clone().to_string(),
    //             precsion.clone().to_string(),
    //             TPR.clone().to_string(),
    //             FPR.clone().to_string(),
    //             TPRnd.clone().to_string(),
    //             FPRnd.clone().to_string()])?;
    //         wtr.flush()?;
    //     }
    //     let rule_set = mining_rule_with_fp_growth("./data/data_final.csv",omega.clone(),loaded_num.clone(),scoring_rate.clone(),1.0,0.2).await?;
    //     let (precsion,TPR,FPR ,TPRnd,FPRnd )= evaluation_policy(&rule_set).await?;
    //     wtr.write_record(&[
    //         loaded_num.clone().to_string(),
    //         scoring_rate.clone().to_string(),
    //         omega.clone().to_string(),
    //         "1.0".clone().to_string(),
    //         precsion.clone().to_string(),
    //         TPR.clone().to_string(),
    //         FPR.clone().to_string(),
    //         TPRnd.clone().to_string(),
    //         FPRnd.clone().to_string()])?;
    //     wtr.flush()?;
    // }

    // let omega = 0.01;
    
    // let scoring_rate = 21.0 /100.0;
    // for obp_it in 1..10{
    //     let obp_rate:f64 = obp_it as f64/10.0;
    //     let rule_set = mining_rule_with_fp_growth("./data/data_final.csv",omega.clone(),loaded_num.clone(),scoring_rate.clone(),obp_rate.clone(),0.1).await?;
    //     let (precsion,TPR,FPR ,TPRnd,FPRnd )= evaluation_policy(&rule_set).await?;
    //     wtr.write_record(&[
    //         loaded_num.clone().to_string(),
    //         scoring_rate.clone().to_string(),
    //         omega.clone().to_string(),
    //         obp_rate.clone().to_string(),
    //         "0.1".to_string(),
    //         precsion.clone().to_string(),
    //         TPR.clone().to_string(),
    //         FPR.clone().to_string(),
    //         TPRnd.clone().to_string(),
    //         FPRnd.clone().to_string()])?;
    //     wtr.flush()?;
    // }
    // let rule_set = mining_rule_with_fp_growth("./data/data_final.csv",omega.clone(),loaded_num.clone(),scoring_rate.clone(),1.0,0.1).await?;
    // let (precsion,TPR,FPR ,TPRnd,FPRnd )= evaluation_policy(&rule_set).await?;
    // wtr.write_record(&[
    //     loaded_num.clone().to_string(),
    //     scoring_rate.clone().to_string(),
    //     omega.clone().to_string(),
    //     "1.0".clone().to_string(),
    //     "0.1".to_string(),
    //     precsion.clone().to_string(),
    //     TPR.clone().to_string(),
    //     FPR.clone().to_string(),
    //     TPRnd.clone().to_string(),
    //     FPRnd.clone().to_string()])?;
    // wtr.flush()?;
    
    // basline_test().await.unwrap();
    // let client = Client::with_uri_str("mongodb://localhost").await.expect("failed to connect mongodb");
    // let transaction_scoring:Collection<Document> = client.database("esx_mining").collection("transacation_scoring");
    // let transaction_scoring_distinct:Collection<Document> = client.database("esx_mining").collection("transacation_scoring_distinct");
    // // let para_space_original:Collection<Document> = client.database("esx_mining").collection("para_space_original");
    // let transaction_original_distinct:Collection<Document> = client.database("esx_mining").collection("transacation_distinct");
    // let transaction_tmp:Collection<Document> = client.database("esx_mining").collection("transacation_tmp");

    // let disk_use_opt = AggregateOptions::builder().allow_disk_use(true).build();
    // transaction_original_distinct.aggregate(generate_union_logs("transacation_scoring_distinct","transacation_tmp"),disk_use_opt.clone()).await.expect("failed to union");
    // let union_num = transaction_tmp.estimated_document_count(None).await.unwrap();
    // let scoring_num =  transaction_scoring_distinct.estimated_document_count(None).await.unwrap();
    // let distinct_len = transaction_original_distinct.estimated_document_count(None).await.unwrap();
    // let mut transaction_len_of_OPP_distinct =scoring_num.clone();
    // println!("union num:{},scoring num:{}",union_num,scoring_num);
    // let mut policy_cover_count_distinct = distinct_len+scoring_num-union_num;
    // println!("dis len:{} , policy_cover_count_d:{}",distinct_len,policy_cover_count_distinct);
    use crate::JobType::*;

    if args.job_type == SupportRateCompare{
        let omega = args.balance_para;
        let scoring_rate = (20.0 as f64) /100.0;
        let support_rates = [0.05,0.1,0.15,0.2,0.3];
        for support_rate in support_rates{
            for obp_it in 1..10{
                let obp_rate:f64 = obp_it as f64/10.0;
                let rule_set = mining_rule_with_fp_growth(args.data_path.as_str(),omega.clone(),loaded_num.clone(),scoring_rate.clone(),obp_rate.clone(),support_rate.clone()).await?;
                let (precsion,TPR,FPR ,TPRnd,FPRnd )= evaluation_policy(&rule_set).await?;
                wtr.write_record(&[
                    loaded_num.clone().to_string(),
                    scoring_rate.clone().to_string(),
                    omega.clone().to_string(),
                    obp_rate.clone().to_string(),
                    support_rate.clone().to_string(),
                    precsion.clone().to_string(),
                    TPR.clone().to_string(),
                    FPR.clone().to_string(),
                    TPRnd.clone().to_string(),
                    FPRnd.clone().to_string()])?;
                wtr.flush()?;
            }
            let rule_set = mining_rule_with_fp_growth(args.data_path.as_str(),omega.clone(),loaded_num.clone(),scoring_rate.clone(),1.0,support_rate.clone()).await?;
            let (precsion,TPR,FPR ,TPRnd,FPRnd )= evaluation_policy(&rule_set).await?;
            wtr.write_record(&[
                loaded_num.clone().to_string(),
                scoring_rate.clone().to_string(),
                omega.clone().to_string(),
                "1.0".clone().to_string(),
                support_rate.clone().to_string(),
                precsion.clone().to_string(),
                TPR.clone().to_string(),
                FPR.clone().to_string(),
                TPRnd.clone().to_string(),
                FPRnd.clone().to_string()])?;
            wtr.flush()?;
        }
    }else{
        let omega = args.balance_para;
        let scoring_rate = 21.0 /100.0;
        for obp_it in 1..10{
            let obp_rate:f64 = obp_it as f64/10.0;
            let rule_set = mining_rule_with_fp_growth(args.data_path.as_str(),omega.clone(),loaded_num.clone(),scoring_rate.clone(),obp_rate.clone(),0.1).await?;
            let (precsion,TPR,FPR ,TPRnd,FPRnd )= evaluation_policy(&rule_set).await?;
            wtr.write_record(&[
                loaded_num.clone().to_string(),
                scoring_rate.clone().to_string(),
                omega.clone().to_string(),
                obp_rate.clone().to_string(),
                "0.1".to_string(),
                precsion.clone().to_string(),
                TPR.clone().to_string(),
                FPR.clone().to_string(),
                TPRnd.clone().to_string(),
                FPRnd.clone().to_string()])?;
            wtr.flush()?;
        }
        let rule_set = mining_rule_with_fp_growth(args.data_path.as_str(),omega.clone(),loaded_num.clone(),scoring_rate.clone(),1.0,0.1).await?;
        let (precsion,TPR,FPR ,TPRnd,FPRnd )= evaluation_policy(&rule_set).await?;
        wtr.write_record(&[
            loaded_num.clone().to_string(),
            scoring_rate.clone().to_string(),
            omega.clone().to_string(),
            "1.0".clone().to_string(),
            "0.1".to_string(),
            precsion.clone().to_string(),
            TPR.clone().to_string(),
            FPR.clone().to_string(),
            TPRnd.clone().to_string(),
            FPRnd.clone().to_string()])?;
        wtr.flush()?;
    }
    

    
    
    // println!("precision:{} ,TPR:{}, FPR:{}",precsion,TPR,FPR);
    // delete_logs_from_rule(&vec2).await?;



    Ok(())
}

async fn basline_test()->Result<()>{
    async fn evaluation_policy_baseline()->Result<(f64,f64,f64,f64,f64)>{
        let start_time = SystemTime::now();
        let client = Client::with_uri_str("mongodb://localhost").await.expect("failed to connect mongodb");
        let transaction_scoring:Collection<Document> = client.database("esx_mining").collection("transacation_scoring");
        let transaction_scoring_distinct:Collection<Document> = client.database("esx_mining").collection("transacation_scoring_distinct");
        // let para_space_original:Collection<Document> = client.database("esx_mining").collection("para_space_original");
        let transaction_original_distinct:Collection<Document> = client.database("esx_mining").collection("transacation_distinct");
        let transaction_tmp1:Collection<Document> = client.database("esx_mining").collection("transacation_tmp1");
        let transaction_tmp2:Collection<Document> = client.database("esx_mining").collection("transacation_tmp2");

        let gid_collection:Collection<Document> = client.database("esx_mining").collection("para_space_gid");


           
    
        
        let disk_use_opt = AggregateOptions::builder().allow_disk_use(true).build();
        transaction_original_distinct.aggregate(generate_union_logs("transacation_scoring","transacation_tmp1"),disk_use_opt.clone()).await.expect("failed to union");
        let union_num = transaction_tmp1.estimated_document_count(None).await.unwrap();
        let distinct_len = transaction_original_distinct.estimated_document_count(None).await.unwrap();
        let mut scoring_num =  transaction_scoring.estimated_document_count(None).await.unwrap();

        
        transaction_original_distinct.aggregate(generate_union_logs_unique("transacation_scoring_distinct","transacation_tmp2"),disk_use_opt.clone()).await.expect("failed to union");
        let union_num_unique = transaction_tmp2.estimated_document_count(None).await.unwrap();
        let scoring_num_distinct =  transaction_scoring_distinct.estimated_document_count(None).await.unwrap();
        let distinct_len = transaction_original_distinct.estimated_document_count(None).await.unwrap();
        let mut transaction_len_of_OPP_distinct =scoring_num.clone();
        let mut policy_cover_count_distinct = distinct_len+scoring_num_distinct-union_num_unique;

        let diff = union_num- union_num_unique;
        scoring_num += diff;
        let transaction_len_of_OPP =scoring_num.clone();
        let mut policy_cover_count = distinct_len+scoring_num-union_num;

        let policy_cover_para_space_count:u64 = distinct_len;
        // let policy_cover_para_space_count = para_space_original.count_documents(policy_doc_for_mongo.clone(),None).await.expect("failed to connect to mongo");
        // let para_space_size:u64 = para_space_original.count_documents(None,None).await.expect("failed to connect to mongo");
    
        let mut gids_cur = gid_collection.find(None, None).await.unwrap();
        let mut gids_vec:Vec<String> = Vec::new();
        // let mut policy_cover_para_space_count = 0;
        let mut para_space_size:u64 = 0;
        while let Some(gid_doc) = gids_cur.try_next().await? {
            let gid = gid_doc.get("gid").unwrap().to_string().replace("\"", "");
            gids_vec.push(gid.clone());
        }
        for gid in gids_vec{
            println!("db name:{}",("para_".to_owned()+gid.clone().as_str()).as_str());
            let giddb:Collection<Document> = client.database("esx_mining").collection(("para_".to_owned()+gid.clone().as_str()).as_str());
            let para_tmp = giddb.estimated_document_count(None).await.unwrap();
            para_space_size = para_space_size + para_tmp;
        }
        
    
    
        // let TruePositiveNum:u64 = policy_cover_count.clone();
        let TruePositiveNum:u64 = policy_cover_count_distinct.clone();
        let TruePositiveNum_no_distinct:u64 = policy_cover_count.clone();
        
        let FalseNegativeNum:u64 = transaction_len_of_OPP_distinct - policy_cover_count_distinct;
        let FalseNegativeNum_no_distinct:u64 = transaction_len_of_OPP - policy_cover_count;
        let FalsePositiveNum:u64 = policy_cover_para_space_count - policy_cover_count_distinct;
        
        
        
        let TrueNegativeNum:u64 = para_space_size - (TruePositiveNum+FalseNegativeNum+FalsePositiveNum);
        println!("TP:{},FN:{},FP:{},TN:{}",TruePositiveNum.clone(),FalseNegativeNum.clone(),FalsePositiveNum.clone(),TrueNegativeNum.clone());
        let mut TrueNegativeNum_no_distinct:i64 = para_space_size as i64 - (TruePositiveNum_no_distinct+FalseNegativeNum_no_distinct+FalsePositiveNum) as i64;
        if TrueNegativeNum_no_distinct < 0{
            TrueNegativeNum_no_distinct = 0;
        }
        
        let mut TPR = 0.0;
        let mut TPR_no_distinct = 0.0;
        if TruePositiveNum+FalseNegativeNum == 0{
            TPR = 1.0;
        }
        else{
            TPR = (TruePositiveNum as f64)/(TruePositiveNum+FalseNegativeNum) as f64;
        }
        let FPR = (FalsePositiveNum as f64)/(FalsePositiveNum + TrueNegativeNum)as f64;
        let precision = (TruePositiveNum as f64)/(TruePositiveNum+FalsePositiveNum) as f64;
        if TruePositiveNum_no_distinct+FalseNegativeNum_no_distinct == 0{
            TPR_no_distinct = 1.0;
        }
        else{
            TPR_no_distinct = (TruePositiveNum_no_distinct as f64)/(TruePositiveNum_no_distinct+FalseNegativeNum_no_distinct) as f64;
        }
        let FPR_no_distinct = (FalsePositiveNum as f64)/((FalsePositiveNum as u64 + TrueNegativeNum_no_distinct as u64)as f64);
        println!("fpn:{}, tnnnd:{}",FalsePositiveNum as f64,TrueNegativeNum_no_distinct as u64 as f64);
        let precision = (TruePositiveNum as f64)/(TruePositiveNum_no_distinct+FalsePositiveNum) as f64;
    
        Ok((precision,TPR,FPR,TPR_no_distinct,FPR_no_distinct))    
    }
    let mut file = OpenOptions::new()
    .write(true)
    .append(true)
    .create(true)
    .open("output_scoring_baseline.csv").unwrap();
    let mut wtr = Writer::from_writer(file);
    println!("start baseline mining..");
    let loaded_num = 2000000;
    let scoring_rate = 21.0 /100.0;
    for obp_it in 1..10{
        let obp_rate:f64 = obp_it as f64/10.0;
        
        let rule_set = mining_rule_with_baseline("./data/data_final.csv",loaded_num.clone(),scoring_rate.clone(),obp_rate.clone()).await?;        
        let (precsion,TPR,FPR ,TPRnd,FPRnd )= evaluation_policy_baseline().await?;
            wtr.write_record(&[
            loaded_num.clone().to_string(),
            scoring_rate.clone().to_string(),
            obp_rate.clone().to_string(),
            precsion.clone().to_string(),
            TPR.clone().to_string(),
            FPR.clone().to_string(),
            TPRnd.clone().to_string(),
            FPRnd.clone().to_string()])?;
        wtr.flush()?;
    }
    let rule_set = mining_rule_with_baseline("./data/data_final.csv",loaded_num.clone(),scoring_rate.clone(),1.0).await?;
    let (precsion,TPR,FPR ,TPRnd,FPRnd )= evaluation_policy_baseline().await?;
    wtr.write_record(&[
        loaded_num.clone().to_string(),
        scoring_rate.clone().to_string(),
        "1.0".clone().to_string(),
        precsion.clone().to_string(),
        TPR.clone().to_string(),
        FPR.clone().to_string(),
        TPRnd.clone().to_string(),
        FPRnd.clone().to_string()])?;
    wtr.flush()?;
    Ok(())
}

fn clustering_test(){
    let n_samples    = 20_000; // # of samples in the example
    let n_dimensions =    200; // # of dimensions in each sample
    let k            =     4; // # of clusters in the result
    let max_iter     =    10; // max number of iterations before the clustering forcefully stops

    println!("start preparing data .. ");
    let data_path = "uop28.csv";
    //load csv
    let  (mut transactions,mut para_space,mut uniqueSet) = load_csv(data_path,1000).expect("failed to load csv data");

    // Generate some random data
    let mut samples: Vec<Vec<f64>> = vec![];
    for _ in 0..n_samples {
        samples.push((0..n_dimensions).map(|_| rand::random()).collect::<Vec<_>>());
    }

    let mut transactions_for_fp = Vec::new();
    for single_vec_transaction in &transactions {
        let mut single_vec_for_fp = Vec::new();
        for single_com_transaction in single_vec_transaction {
            single_vec_for_fp.push(single_com_transaction.as_str().as_bytes());
        }
        transactions_for_fp.push(single_vec_for_fp);
    }
    let trans_len = transactions.len().clone();

    println!("start clustering .. samples:{:?}",samples);

    let mut model = GSDMM::new(0.1, 0.1, 8, 30, uniqueSet, transactions);

    model.fit();


// actually perform the clustering
//     let clustering = kmeans(k, &transactions_for_fp, max_iter);
//
//
//     println!("membership: {:?}", clustering.membership);
//     println!("centroids : {:?}", clustering.centroids);
}


async fn mining_rule_with_baseline(data_path:&str,loaded_num:u64,scoring_rate:f64,obp_rate:f64)->Result<()>{
    // let data_path = "uop28.csv";
    //load csv

    let start_time = SystemTime::now();
    let  (mut transactions,mut uniqueSet) = load_csv_to_mongodb_with_scoring(data_path,loaded_num,scoring_rate,obp_rate).await.expect("failed to load csv data");
    println!("transaction len ={}",transactions.len());
    transactions.clear();
    
    
    let client = Client::with_uri_str("mongodb://localhost").await.expect("failed to connect mongodb");

    let mut transcation_new:Vec<Document> = Vec::new();
    let mut transcation_new:Vec<Vec<String>> = Vec::new();
    let mut uniqueSet:HashSet<String> = Default::default();
    let mut transaction_docs:Vec<Document> = Vec::new();
    let mut transaction_docs_distinct:Vec<Document> = Vec::new();
    // Loop through the results and print a summary and the comments:
    
    let transaction_original_distinct:Collection<EsxLog> = client.database("esx_mining").collection("transacation_distinct");
    // let td_len = transaction_original_distinct.estimated_document_count(None).await.unwrap();
    // let pb = ProgressBar::new(td_len.clone() as u64);
    // let mut cur = transaction_original_distinct.find(None, None).await.unwrap();
    // // let mut influence_tmp = vec![];
    // // let mut para_space_size = 0;
    // pb.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len}({per_sec},{eta})")
    //     .unwrap()
    //     .progress_chars("#>-"));
    // while let Some(mut single_esx_log) = cur.try_next().await.expect("failed to change document to EsxLog") {
    //     pb.inc(1);
    //     // single_esx_log.remove("_id").unwrap();
    //     // println!("{:?}",single_esx_log.clone());
    //     transcation_new.push(single_esx_log.to_logs_vec());
    // }
    // pb.finish();

    let end_time = SystemTime::now().duration_since(start_time).unwrap().as_secs_f64();
    println!("single baseline time use:{}",end_time);

    return Ok(());


}




async fn mining_rule_with_fp_growth(data_path:&str,omega:f64,loaded_num:u64,scoring_rate:f64,obp_rate:f64,support_rate:f64)->Result<Vec<Vec<String>>>{
    // let data_path = "uop28.csv";
    //load csv
    // let  (mut transactions,mut para_space,mut uniqueSet) = load_csv(data_path,1000).expect("failed to load csv data");
    // let  (mut transactions,mut para_space,mut uniqueSet) = load_csv_to_mongodb(data_path,100000).await.expect("failed to load csv data");
    let  (mut transactions,mut uniqueSet) = load_csv_to_mongodb_with_scoring(data_path,loaded_num,scoring_rate,obp_rate).await.expect("failed to load csv data");
    println!("transaction len ={}",transactions.len());
    let  mutcount = 0;
    // for item in &para_space{
    //     ;
    // }
    let mut rule_vec :Vec<String>= Vec::new();
    let mut fs_rule_vec:Vec<FilesystemRule> = Vec::new();
    let mut rule_set_str = Vec::new();
    // let mut pattern_over_rate = HashMap::new();
    
    let client = Client::with_uri_str("mongodb://localhost").await.expect("failed to connect mongodb");
    let transaction_original:Collection<Document> = client.database("esx_mining").collection("transacation_original");
    let transaction_original_distinct:Collection<Document> = client.database("esx_mining").collection("transacation_distinct");
    let gid_collection:Collection<Document> = client.database("esx_mining").collection("para_space_gid");
    // 存储overate相关信息的collection
    let overate_collection:Collection<Document> = client.database("esx_mining").collection(("overate_".to_string()+loaded_num.to_string().as_str()).as_str());
    
    let para_space_size = client.database("esx_mining").collection::<Document>("para_space_original").estimated_document_count(None).await.expect("failed to connect para space data in mongo");
    let mut gids_cur = gid_collection.find(None, None).await.unwrap();
    let mut gids_vec:Vec<String> = Vec::new();
 
    while let Some(gid_doc) = gids_cur.try_next().await? {

        let gid = gid_doc.get("gid").unwrap().to_string().replace("\"", "");
        gids_vec.push(gid.clone());
    }
    let mut para_space_size =0;
    for gid in &gids_vec{
        let giddb:Collection<Document> = client.database("esx_mining").collection(("para_".to_owned()+gid.clone().as_str()).as_str());
        let para_tmp = giddb.estimated_document_count(None).await.unwrap();
        para_space_size = para_space_size + para_tmp;
    }
    println!("para space size:{}",para_space_size.clone());
    let start_time = SystemTime::now();
    while !&transactions.is_empty()  {
        let mut transactions_for_fp = Vec::new();
        for single_vec_transaction in &transactions {
            let mut single_vec_for_fp = Vec::new();
            for single_com_transaction in single_vec_transaction {
                single_vec_for_fp.push(single_com_transaction.as_str());
            }
            transactions_for_fp.push(single_vec_for_fp);
        }
        let trans_len = transactions.len().clone();
        if trans_len == 0 as usize{
            break;
        }
        let mut minimum_support = (support_rate*trans_len.clone()as f64) as usize;
        // if trans_len.clone()<100{
        //     minimum_support = 1;
        // }

        let fp_growth_str = FPGrowth::new(transactions_for_fp.clone(), minimum_support);
        let mut result = fp_growth_str.find_frequent_patterns();

        println!("The number of results: {}", result.frequent_patterns_num());
        if result.frequent_patterns_num() == 0{
            minimum_support = 1;
            let fp_growth_str = FPGrowth::new(transactions_for_fp, minimum_support);
            result = fp_growth_str.find_frequent_patterns();
        }
        let mut max_support: usize = 0;
        let mut max_c_score =NEG_INFINITY;
        let mut most_frequent_pattern: Vec<&str> = Vec::new();
        let pattern_len =result.frequent_patterns().len().clone();
        let pb = ProgressBar::new(pattern_len.clone() as u64);
        // let mut influence_tmp = vec![];
        // let mut para_space_size = 0;
        pb.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len}({per_sec},{eta})")
            .unwrap()
            .progress_chars("#>-"));
        let mut frequent_pattern_to_calculate = 0;
        // let calculation_limit = 40;
        for (frequent_pattern, support) in result.frequent_patterns().iter() {
            pb.inc(1);
            let len = &frequent_pattern.len();

            if frequent_pattern.contains(&"logtype:FILE") || frequent_pattern.contains(&"logtype:NET") {
                
                // 防止过多的计算
                // if frequent_pattern_to_calculate >= calculation_limit {
                //     break;
                // }
                frequent_pattern_to_calculate = frequent_pattern_to_calculate + 1;


                let frequent_pattern_tmp = frequent_pattern.clone();
                let log_entries_len = trans_len.clone();
                // let (coverage_rate_fn,unique_logs_count,cover_logs_count) = calculating_coverage_rate(&transactions,&most_frequent_pattern).expect("failed to calculate coverage rate");
                let (coverage_rate_fn,unique_logs_count,cover_logs_count) = calculating_coverage_rate_mongo(&frequent_pattern_tmp).await.expect("failed to calculate coverage rate");

                let (mut influence_rate_from_space, mut unique_space_count) =(0.0,0);

                if frequent_pattern_tmp.contains(&"logtype:FILE") {
                    let rule_tmp = generate_fsrule_from_item(&frequent_pattern_tmp).unwrap();
                    let op_overate = overate_collection.find_one(doc!{
                        "pattern":rule_tmp.to_string()
                    }, None).await.unwrap();
                    match op_overate {
                        Some(op_tun) =>{
                            influence_rate_from_space = op_tun.get("influence_rate_from_space").unwrap().as_f64().unwrap();
                            unique_space_count = op_tun.get("unique_space_count").unwrap().as_i32().unwrap() as usize;
                        }
                        None=>{
                            (influence_rate_from_space,unique_space_count) = calculating_overate_rate_mongo(&frequent_pattern_tmp,para_space_size as f64).await.expect("failed to calculate overate");
                            overate_collection.insert_one(doc!{
                                "pattern":rule_tmp.to_string(),
                                "influence_rate_from_space":influence_rate_from_space,
                                "unique_space_count":unique_space_count as i32
                            }, None).await.unwrap();
                        }
                    }
                    // let op_overate = pattern_over_rate.get(&*rule_tmp.create_influence_vec());
                    // influence_tmp = rule_tmp.create_influence_vec();
                    // match op_overate {
                    //     Some(op_tun)=>{
                    //         (influence_rate_from_space,unique_space_count) = *op_tun;
                    //     }
                    //     None => {
                    //         (influence_rate_from_space,unique_space_count) = calculating_overate_rate_mongo(&frequent_pattern_tmp,para_space_size as f64).await.expect("failed to calculate overate");
                    //         // (influence_rate_from_space,unique_space_count) = calculating_over_rate(&para_space,&most_frequent_pattern).expect("failed to calculate overate");
                    //         pattern_over_rate.insert(rule_tmp.create_influence_vec(),(influence_rate_from_space,unique_space_count));
                    //     }

                    // }
                }
                else if frequent_pattern_tmp.contains(&"logtype:NET") {
                    let rule_tmp = generate_netrule_from_item(&frequent_pattern_tmp).unwrap();
                    let op_overate = overate_collection.find_one(doc!{
                        "pattern":rule_tmp.to_string()
                    }, None).await.unwrap();
                    match op_overate {
                        Some(op_tun) =>{
                            influence_rate_from_space = op_tun.get("influence_rate_from_space").unwrap().as_f64().unwrap();
                            unique_space_count = op_tun.get("unique_space_count").unwrap().as_i32().unwrap() as usize;
                        }
                        None=>{
                            (influence_rate_from_space,unique_space_count) = calculating_overate_rate_mongo(&frequent_pattern_tmp,para_space_size as f64).await.expect("failed to calculate overate");
                            overate_collection.insert_one(doc!{
                                "pattern":rule_tmp.to_string(),
                                "influence_rate_from_space":influence_rate_from_space,
                                "unique_space_count":unique_space_count as i32
                            }, None).await.unwrap();
                        }
                    }
                    // let op_overate = pattern_over_rate.get(&*rule_tmp.create_influence_vec());
                    // influence_tmp = rule_tmp.create_influence_vec();
                    // match op_overate {
                    //     Some(op_tun)=>{
                    //         (influence_rate_from_space,unique_space_count) = *op_tun;
                    //     }
                    //     None => {
                    //         // (influence_rate_from_space,unique_space_count) = calculating_over_rate(&para_space,&most_frequent_pattern).expect("failed to calculate overate");
                    //         (influence_rate_from_space,unique_space_count) = calculating_overate_rate_mongo(&frequent_pattern_tmp,para_space_size as f64).await.expect("failed to calculate overate");
                    //         pattern_over_rate.insert(rule_tmp.create_influence_vec(),(influence_rate_from_space,unique_space_count));
                    //     }
                    // }
                }else {
                    continue;
                }

                // let (influence_rate_from_space,unique_space_count) = calculating_over_rate(&para_space,&most_frequent_pattern).expect("failed to calculate overate");
                let overate = (unique_space_count as f64-unique_logs_count as f64)/(para_space_size as f64);
                let overate_not_distinct = (unique_space_count as f64-cover_logs_count as f64)/(para_space_size as f64);

                let over_assignment_total = unique_space_count as f64-unique_logs_count as f64;
                // let omega = 0.5;
                let Qrul_count = (unique_logs_count as f64)*(1.0-((omega * over_assignment_total)/unique_logs_count as f64));

                let l_distance = 0.0-(log_entries_len as f64 - cover_logs_count)-(omega*over_assignment_total);
                let harmonic_mean = (1.0+(omega*omega)) * ((overate*coverage_rate_fn)/(((omega*omega)*overate)+coverage_rate_fn));
                
                let Qrul_freq = (log_entries_len as f64 )*(1.0-((omega*over_assignment_total)/unique_logs_count as f64));
                let e_score = coverage_rate_fn + omega * (1.0-overate)+ omega * (1.0-overate_not_distinct);
                let C_scores = (cover_logs_count as f64 / log_entries_len as f64) + omega*(1.0-overate_not_distinct);
                // let c_score = l_distance;
                let c_score = e_score;
                println!("frequent pattern:{:?} ,influence rate: {} , overate :{},overate_n_d:{} ,c_score:{}",&frequent_pattern,coverage_rate_fn,overate,overate_not_distinct,c_score);

                if c_score>max_c_score {
                    max_c_score = c_score;
                    most_frequent_pattern = frequent_pattern.clone();
                    max_support = *support;
                }
            }
            // println!("{:?} {}", frequent_pattern, support);
        }
        pb.finish_and_clear();
        println!("most frequent:{:?} , max support={},max c_score={}", most_frequent_pattern, max_support,max_c_score);

        //做GC
        // let ret = pattern_over_rate.remove(&influence_tmp);


        let mut transacation_back = transactions.clone();
        let trans_len = &transactions.len();


        
        // let (coverage_rate_fn,unique_logs_count,cover_logs_count) = calculating_coverage_rate(&transactions,&most_frequent_pattern).expect("failed to calculate coverage rate");
        // let (influence_rate_from_space,unique_space_count,cover_space_count) = calculating_coverage_rate(&para_space,&most_frequent_pattern).expect("failed to calculate overate");
        // let overate = (unique_space_count as f64-unique_logs_count as f64)/(para_space.len() as f64);
        let trans_after_delete =  delete_logs_from_rule(&most_frequent_pattern).await.unwrap();
        
        if most_frequent_pattern.contains(&"logtype:FILE"){
            let fsrule = generate_fsrule_from_item(&most_frequent_pattern).unwrap();
            rule_set_str.push(fsrule.create_influence_vec());
        }else if most_frequent_pattern.contains(&"logtype:NET") { 
            let netrule = generate_netrule_from_item(&most_frequent_pattern).unwrap();
            rule_set_str.push(netrule.create_influence_vec());
        }else{
            continue;
        }


        

        transacation_back = trans_after_delete;
        let len_after_delete = &transacation_back.len();
        println!("len of trans:{},len of back:{}", &trans_len.borrow(), len_after_delete);
        let end_time = SystemTime::now().duration_since(start_time).unwrap().as_secs_f64();
        println!("single mining time use:{}",end_time);
        // let converage_rate = (*trans_len as f64-*len_after_delete as f64)/(*trans_len as f64);
        // println!("coverage_rate = :{} coverage_rate_fn = {},overate ={}",converage_rate,coverage_rate_fn,overate);
        
        transactions = transacation_back;
    }
    println!("length of policy = {},rule set = {:?}",rule_set_str.len(),rule_set_str.clone());
    Ok(rule_set_str)
}







//load csv
//input: csv file path ,loaded numbers
//output: original transaction and original parameter space

fn load_csv(data_path:&str,loaded_num:u64) -> Result<(Vec<Vec<String>>,Vec<Vec<String>>,HashSet<String>)> {
    let mut transactions2 = Vec::new();
    let mut f = File::open(data_path).expect("expect a csv file");

    // 建立 CSV 读取器并且遍历每一条记录。
    // let mut rdr = csv::Reader::from_reader(f);
    // let mut reader = ReaderBuilder::new().delimiter(b'\t').from_reader(f);
    let mut reader = ReaderBuilder::new().delimiter(b',').from_reader(f);

    let mut if_broken = false;
    //create parameter space
    let mut para_space:Vec<Vec<String>> = Vec::new();
    let mut unique_uid_file:Vec<String> = Vec::new();
    let mut unique_gid_file:Vec<String> = Vec::new();
    let mut unique_gid_uid_file:Vec<(String,String)> = Vec::new();
    let mut unique_op_file:Vec<String> = Vec::new();
    let mut unique_res_file:Vec<String> = Vec::new();
    let unique_type_file =vec!["logtype:FILE".to_string()];

    
    let mut unique_uid_net:Vec<String> = Vec::new();
    let mut unique_gid_net:Vec<String> = Vec::new();
    let mut unique_op_net:Vec<String> = Vec::new();
    let mut unique_res_net:Vec<String> = Vec::new();
    let unique_type_net =vec!["logtype:NET".to_string()];
    let mut unique_gid_uid_net:Vec<(String,String)> = Vec::new();

    // let records_len = &reader.records().count();
    println!("start coverting");
    let mut count = 0;
    let pb = ProgressBar::new(loaded_num);
    pb.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len}({per_sec},{eta})")
        .unwrap()
        .progress_chars("#>-"));
    for result in reader.records() {
        pb.inc(1);
        
        if count == loaded_num{
            break;
        }
        count = count + 1;

        // 遍历器会返回Result<StringRecord, Error>，所以我们在这里检查是否有错误。
        // let record = result.expect("is not 5");
        let mut record;
        match result {
            Ok(r)=>{record = r} ,
            Err(e)=>{
                continue
            }
        }
        // println!("{:?}",record);
        //clean data
        let mut single_vec = Vec::new();
        if !&record[2].contains("FILE")&&!&record[2].contains("NET") {
            println!("fail record:{:?}",&record);
            continue;
        }
        if_broken = false;

        if record.len() == 5 {
            let mut count = 0;
            if record[2].borrow().contains("FILE"){
                let mut uid_tmp = "".to_string();
                let mut gid_tmp = "".to_string();
                for single_com in &record{
                    // let mut single_str = single_com.to_string();

                    let mut single_str = "".to_string();
                    if single_com.is_empty(){
                        if_broken = true;
                        break;
                    }
                    match count {
                        0 =>{
                            single_str = (String::from("UID:") +single_com);
                            if !unique_uid_file.contains(&single_str){
                                unique_uid_file.push(single_str.clone());
                            }
                            uid_tmp = single_str.clone();
                        },
                        1 =>{
                            let mut hasher = DefaultHasher::new();
                            hasher.write(single_com.as_bytes());
                            let uid = hasher.finish() as i32;
                            single_str = String::from("GID:") +uid.to_string().as_str();
                            if !unique_gid_file.contains(&single_str){
                                unique_gid_file.push(single_str.clone());
                            }
                            gid_tmp = single_str.clone();
                        },
                        2 =>{
                            single_str = String::from("logtype:") +single_com;
                        },
                        3 =>{
                            single_str = (String::from("op:") +single_com);
                            if !unique_op_file.contains(&single_str){
                                unique_op_file.push(single_str.clone());
                            }
                        },
                        4 =>{
                            single_str = (String::from("res:") +single_com);
                            if !unique_res_file.contains(&single_str){
                                unique_res_file.push(single_str.clone());
                            }
                        },
                        _ => {}
                    }
                    // println!("{}",single_com);

                    single_vec.push(single_str);
                    count  = count + 1;
                }
                if if_broken{
                    continue;
                }
                if !unique_gid_uid_file.contains(&(gid_tmp.clone(), uid_tmp.clone())){
                    unique_gid_uid_file.push((gid_tmp.clone(),uid_tmp.clone()));
                }
            }
            else if record[2].borrow().contains("NET"){
                let mut uid_tmp = "".to_string();
                let mut gid_tmp = "".to_string();
                for single_com in &record{
                    // let mut single_str = single_com.to_string();
                    let mut single_str = "".to_string();
                    if single_com.is_empty(){
                        if_broken = true;
                        break;
                    }
                    match count {
                        0 =>{
                            single_str = (String::from("UID:") +single_com);
                            // if !unique_uid_net.contains(&single_str){
                            //     unique_uid_net.push(single_str.clone());
                            // }
                            uid_tmp = single_str.clone();
                        },
                        1 =>{
                            single_str = (String::from("GID:") +single_com);
                            // if !unique_gid_net.contains(&single_str){
                            //     unique_gid_net.push(single_str.clone());
                            // }
                            gid_tmp = single_str.clone();
                        },
                        2 =>{
                            single_str = (String::from("logtype:") +single_com);
                        },
                        3 =>{
                            single_str = (String::from("op:") +single_com);
                            if !unique_op_net.contains(&single_str){
                                unique_op_net.push(single_str.clone());
                            }
                        },
                        4 =>{
                            single_str = (String::from("res:") +single_com);
                            if !unique_res_net.contains(&single_str){
                                unique_res_net.push(single_str.clone());
                            }
                        },
                        _ => {}
                    }
                    // println!("{}",single_com);
                    single_vec.push(single_str);
                    count  = count + 1;
                }
                if if_broken{
                    continue;
                }
                if !unique_gid_uid_net.contains(&(gid_tmp.clone(), uid_tmp.clone())){
                    unique_gid_uid_net.push((gid_tmp.clone(),uid_tmp.clone()));
                }

            }
            transactions2.push(single_vec);
        }

    }
    println!("start creating para space");
    //create file parameter space
    for (gid,uid) in &unique_gid_uid_file{
        for op in &unique_op_file{
            for res in &unique_res_file{
                for utype in &unique_type_file {
                    para_space.push(vec![uid.clone(), gid.clone(),utype.clone(),op.clone(),res.clone()]);
                }
            }
        }
    }

    //create net parameter space
    for (gid,uid) in &unique_gid_uid_net{
        for op in &unique_op_net{
            for res in &unique_res_net{
                for utype in &unique_type_net {
                    para_space.push(vec![uid.clone(), gid.clone(),utype.clone(),op.clone(),res.clone()]);
                }
            }
        }
    }
    //create unique HashSet
    let mut uniqueSet:HashSet<String> = Default::default();
    for gid in &unique_gid_file{
        uniqueSet.insert(gid.clone());
    }
    for gid in &unique_gid_net{
        uniqueSet.insert(gid.clone());
    }
    for uid in &unique_uid_file{
        uniqueSet.insert(uid.clone());
    }
    for uid in &unique_uid_net{
        uniqueSet.insert(uid.clone());
    }
    for res in &unique_res_file{
        uniqueSet.insert(res.clone());
    }
    for res in &unique_res_net{
        uniqueSet.insert(res.clone());
    }
    for op in &unique_op_file{
        uniqueSet.insert(op.clone());
    }
    for op in &unique_op_net{
        uniqueSet.insert(op.clone());
    }
    for utype in &unique_type_file{
        uniqueSet.insert(utype.clone());
    }
    for utype in &unique_type_net{
        uniqueSet.insert(utype.clone());
    }



    // println!("space_parameter len:{}",&para_space.len());
    pb.finish_with_message("mining data loaded");
    Ok((transactions2,para_space,uniqueSet))
}




async fn load_csv_with_para_and_backup_loaded(data_path:&str,loaded_num:u64,mongo_backup:&str)->Result<(Vec<Vec<String>>,HashSet<String>)>{

    let client = Client::with_uri_str("mongodb://localhost").await.expect("failed to connect mongodb");
    let transaction_original:Collection<Document> = client.database("esx_mining").collection("transacation_original");
    let transaction_original_distinct:Collection<Document> = client.database("esx_mining").collection("transacation_distinct");
    let transaction_original_backup:Collection<Document> = client.database("esx_mining").collection("transacation_original_backup");    
    let transaction_original_distinct_backup:Collection<Document> = client.database("esx_mining").collection("transacation_distinct_backup");
    let transacations = load_csv_data_pure(data_path, loaded_num).unwrap();
    transaction_original_backup.delete_many(doc!{}, None).await.unwrap();
    transaction_original_distinct_backup.delete_many(doc!{}, None).await.unwrap();
    let mut doc_set = Vec::new();
    let pb = ProgressBar::new(transacations.len().clone() as u64);
    pb.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len}({per_sec},{eta})")
        .unwrap()
        .progress_chars("#>-"));
    for transaction in &transacations{
        pb.inc(1);
        let new_doc = doc! {
        "uid": transaction[0].clone(),
        "gid": transaction[1].clone(),
        "logtype": transaction[2].clone(),
        "op": transaction[3].clone(),
        "res":transaction[4].clone()
        };

        doc_set.push(new_doc.clone());
        if doc_set.len() >=100000 {
            transaction_original_backup.insert_many(doc_set.clone(),None).await.expect("failed to insert data to mongodb");
            doc_set.clear();
        }
    }
    match transaction_original_backup.insert_many(doc_set,None).await{
        Ok(r)=>{},
        Err(e) =>{}
    };
    pb.finish_and_clear();
    //insert backup distinct
    let disk_use_opt = AggregateOptions::builder().allow_disk_use(true).build();

    transaction_original_backup.aggregate(generate_distinct_logs("transacation_distinct_backup"),disk_use_opt.clone()).await.expect("failed to deduplicate");

    println!("start creating para space");
    //获取每个field的distinct数据
    let distinct_item_pipe_line = vec![
        doc! {
            "$group": {
                "_id":{
                    "logtype":"$logtype","item":"$gid"
                }
             }
        }
   ];
   let mut all_distinct_item:Vec<Vec<String>> = Vec::new();
   let mut uid_gid_item:Vec<Vec<(String,String)>> = Vec::new();

   for logtype in vec!["logtype:FILE","logtype:NET"]{
        for item_type in vec!["$op","$res"]{
            let mut distinct_items:Vec<String> = Vec::new();
            let distinct_item_pipe_line = vec![
                doc! {
                    "$group": {
                        "_id":{
                            "logtype":"$logtype","item":item_type.clone()
                        }
                    }
                },doc!{"$match":{"_id.logtype":logtype.clone()}}
            ];
            let mut distinct_year = transaction_original_distinct_backup.aggregate(distinct_item_pipe_line, disk_use_opt.clone()).await.unwrap();

            while let Some(single_item) = distinct_year.try_next().await.unwrap() {
               // let doc: YearSummary = bson::from_document(single_movie?)?;
               // movie_vec.push(single_movie.clone());
               let item = single_item.get("_id").unwrap().as_document().unwrap().get("item").unwrap().to_string().replace("\"", "");
            //    println!("{}",item);
               // println!("found or* {}", single_year.get("_id").unwrap().as_document().unwrap().get("title").unwrap());
                distinct_items.push(item.clone());
           }
           all_distinct_item.push(distinct_items.clone());
        }
    }

    for logtype in vec!["logtype:FILE","logtype:NET"]{
            let mut distinct_ugid:Vec<(String,String)> = Vec::new();
            let distinct_item_pipe_line = vec![
                doc! {
                    "$group": {
                        "_id":{
                            "logtype":"$logtype","uid":"$uid","gid":"$gid",
                        }
                    }
                },doc!{"$match":{"_id.logtype":logtype.clone()}}
            ];
            let mut distinct_year = transaction_original_distinct_backup.aggregate(distinct_item_pipe_line, disk_use_opt.clone()).await.unwrap();

            while let Some(single_item) = distinct_year.try_next().await.unwrap() {
               // let doc: YearSummary = bson::from_document(single_movie?)?;
               // movie_vec.push(single_movie.clone());
               let uid = single_item.get("_id").unwrap().as_document().unwrap().get("uid").unwrap().to_string().replace("\"", "");
               let gid = single_item.get("_id").unwrap().as_document().unwrap().get("gid").unwrap().to_string().replace("\"", "");

            //    println!("{}",item);
               // println!("found or* {}", single_year.get("_id").unwrap().as_document().unwrap().get("title").unwrap());
                distinct_ugid.push((uid.clone(),gid.clone()));
           }
           uid_gid_item.push(distinct_ugid.clone());
        
    }


    let mut gid_docs:Vec<Document> =Vec::new();
    let mut gid_uid_map:HashMap<String,Vec<String>> = HashMap::new();
    let mut gid_collection_map:HashMap<String,Collection<Document>> = HashMap::new();
    //为para_space 按照gid进行分表
    for uid_gid_array in &uid_gid_item{
        for (uid,gid) in uid_gid_array{
            
            match gid_uid_map.get_mut(gid){
                Some(uid_vec) =>{uid_vec.push(uid.clone())},
                None=>{gid_uid_map.insert(gid.clone(),vec![uid.clone()]);}
            }
            // gid_set.insert(gid.clone());
        }
    }
    let gid_para:Collection<Document> = client.database("esx_mining").collection("para_space_gid");
    gid_para.delete_many(doc!{}, None).await.unwrap();

    for gid in gid_uid_map.keys(){
        let giddb:Collection<Document> = client.database("esx_mining").collection(("para_".to_string()+gid.clone().as_str()).as_str());
        // match giddb.delete_many(doc!{}, None).await{
        //     Ok(r)=>{}
        //     Err(e)=>{}
        // }
        giddb.delete_many(doc!{}, None).await.unwrap();
        gid_docs.push(doc!{"gid":gid.clone()});
        gid_collection_map.insert(gid.clone(), giddb.clone());
    }
    gid_para.insert_many(gid_docs, None).await.unwrap();

    

    let mut para_count_1 = 0;
    let para_space_original:Collection<Document> = client.database("esx_mining").collection("para_space_original");
    para_space_original.delete_many(doc!{}, None).await.unwrap();
    // let mut para_doc_set =Vec::new();
    let para_limit = 1000000;
    let logtype_tmp = "logtype:FILE";
    println!("len of para space to load:{}",all_distinct_item[0].len()*all_distinct_item[1].len()*uid_gid_item[0].len());
    let para_len =all_distinct_item[0].len()*all_distinct_item[1].len()*uid_gid_item[0].len();
    
    // let pb = ProgressBar::new(para_len.clone() as u64);
    // pb.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len}({per_sec},{eta})")
    //     .unwrap()
    //     .progress_chars("#>-"));
    
    // for (uid,gid) in &uid_gid_item[0]{
    //         for op in &all_distinct_item[0]{
    //             for res in &all_distinct_item[1]{
    //                 pb.inc(1);
    //                 let new_doc = doc! {
    //                     "uid": uid.clone(),
    //                     "gid": gid.clone(),
    //                     "logtype": logtype_tmp.clone(),
    //                     "op": op.clone(),
    //                     "res":res.clone()
    //                     };
                        
    //                     para_doc_set.push(new_doc.clone());
    //                     // para_count_1 = para_count_1 +1;
    //                     if para_doc_set.len() >=para_limit {
    //                         para_space_original.insert_many(para_doc_set.clone(),None).await.expect("failed to insert data to mongodb");
    //                         para_doc_set.clear();
    //                     }
    //             }
    //         }
    // }
    // pb.finish();
    //按照分表查询hashmap
    let pb = ProgressBar::new(para_len.clone() as u64);
    pb.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len}({per_sec},{eta})")
        .unwrap()
        .progress_chars("#>-"));
    
    //按照分表载入mongodb
    for (gid,uid_vec) in &gid_uid_map{
        let mut para_gid_doc_set:Vec<Document> = Vec::new();
        let gid_para_collection = gid_collection_map.get(gid).unwrap();
        let mut index_option = IndexOptions::builder().unique(true).build();
   
   
        let index_model = IndexModel::builder()
           .keys(doc!{"uid":1,"op":1,"res":1}).options(index_option)
           .build();
        gid_para_collection.create_index(index_model, None).await.unwrap();
        for uid in uid_vec{
            for op in &all_distinct_item[0]{
                for res in &all_distinct_item[1]{
                    pb.inc(1);
                    let new_doc = doc! {
                        "uid": uid.clone(),
                        "gid": gid.clone(),
                        "logtype": logtype_tmp.clone(),
                        "op": op.clone(),
                        "res":res.clone()
                        };
                        
                        para_gid_doc_set.push(new_doc.clone());
                        // para_count_1 = para_count_1 +1;
                        if para_gid_doc_set.len() >=para_limit {
                            gid_para_collection.insert_many(para_gid_doc_set.clone(),None).await.expect("failed to insert data to mongodb");
                            para_gid_doc_set.clear();
                        }
                }
            }
        }
        match gid_para_collection.insert_many(para_gid_doc_set.clone(),None).await{
            Ok(r) =>{},
            Err(e) =>{}
        }
        para_gid_doc_set.clear();
    }
    
    pb.finish();

    // let logtype_tmp = "logtype:NET";
    
    // for (uid,gid) in &uid_gid_item[1]{
    //         for op in &all_distinct_item[2]{
    //             for res in &all_distinct_item[3]{
    //                 let new_doc = doc! {
    //                     "uid": uid.clone(),
    //                     "gid": gid.clone(),
    //                     "logtype": logtype_tmp.clone(),
    //                     "op": op.clone(),
    //                     "res":res.clone()
    //                     };
    //                     para_doc_set.push(new_doc.clone());
    //                     if para_doc_set.len() >=para_limit {
    //                         para_space_original.insert_many(para_doc_set.clone(),None).await.expect("failed to insert data to mongodb");
    //                         para_doc_set.clear();
    //                     }
    //             }
    //         }
        
    // }
    // match para_space_original.insert_many(para_doc_set.clone(),None).await{
    //     Ok(r)=>{},
    //     Err(e)=>{}
    // };
    // para_doc_set.clear();

    //按照分表载入mongodb
    for (gid,uid_vec) in &gid_uid_map{
        let mut para_gid_doc_set:Vec<Document> = Vec::new();
        let gid_para_collection = gid_collection_map.get(gid).unwrap();

        for uid in uid_vec{
            for op in &all_distinct_item[2]{
                for res in &all_distinct_item[3]{
                    pb.inc(1);
                    let new_doc = doc! {
                        "uid": uid.clone(),
                        "gid": gid.clone(),
                        "logtype": logtype_tmp.clone(),
                        "op": op.clone(),
                        "res":res.clone()
                        };
                        
                        para_gid_doc_set.push(new_doc.clone());
                        // para_count_1 = para_count_1 +1;
                        if para_gid_doc_set.len() >=para_limit {
                            gid_para_collection.insert_many(para_gid_doc_set.clone(),None).await.expect("failed to insert data to mongodb");
                            para_gid_doc_set.clear();
                        }
                }
            }
        }
        match gid_para_collection.insert_many(para_gid_doc_set.clone(),None).await{
            Ok(r)=>{},
            Err(e)=>{}
        }
        para_gid_doc_set.clear();
    }


    let mut unique_set = HashSet::new();
    unique_set.insert("logtype:FILE".to_string());
    for itemset in &all_distinct_item{
        for item in itemset{
            unique_set.insert(item.clone());
        }
    }
    let mut para_len_after_shard:u64 = 0;
    for (gid,db) in gid_collection_map{
        let len = db.estimated_document_count(None).await.unwrap();
        if len !=0{
            para_len_after_shard = para_len_after_shard + len;
        }
    }
    println!("para_len_After:{}",para_len_after_shard);


    Ok((transacations,unique_set))
}


fn load_csv_data_pure(data_path:&str,loaded_num:u64) ->Result<Vec<Vec<String>>>{
    
    let mut transactions:Vec<Vec<String>> = Vec::new();
    let mut f = File::open(data_path).expect("expect a csv file");

    // 建立 CSV 读取器并且遍历每一条记录。
    // let mut rdr = csv::Reader::from_reader(f);
    // let mut reader = ReaderBuilder::new().delimiter(b'\t').from_reader(f);
    let mut reader = ReaderBuilder::new().delimiter(b',').from_reader(f);
    let pb = ProgressBar::new(loaded_num);
    pb.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len}({per_sec},{eta})")
        .unwrap()
        .progress_chars("#>-"));
    let mut count_all = 0;
    let mut after_count = 0;
    let mut if_broken = false;
    
    for result in reader.records() {
        
        if count_all == loaded_num{
            break;
        }
        count_all = count_all + 1;
        pb.inc(1);
        // 遍历器会返回Result<StringRecord, Error>，所以我们在这里检查是否有错误。
        // let record = result.expect("is not 5");
        let mut record;
        match result {
            Ok(r)=>{record = r} ,
            Err(e)=>{
                continue
            }
        }
        // println!("{:?}",record);
        //clean data
        let mut single_vec = Vec::new();
        if !&record[2].contains("FILE")&&!&record[2].contains("NET") {
            println!("fail record:{:?}",&record);
            continue;
        }
        if_broken = false;

        if record.len() == 5 {
            let mut count = 0;
            if record[2].borrow().contains("FILE"){
                let mut uid_tmp = "".to_string();
                let mut gid_tmp = "".to_string();
                for single_com in &record{
                    // let mut single_str = single_com.to_string();

                    let mut single_str = "".to_string();
                    if single_com.is_empty(){
                        if_broken = true;
                        break;
                    }
                    match count {
                        0 =>{
                            single_str = (String::from("UID:") +single_com);
                        },
                        1 =>{
                            let mut hasher = DefaultHasher::new();
                            hasher.write(single_com.as_bytes());
                            let uid = hasher.finish() as i32;
                            single_str = String::from("GID:") +uid.to_string().as_str();
                        },
                        2 =>{
                            single_str = String::from("logtype:") +single_com;
                        },
                        3 =>{
                            single_str = (String::from("op:") +single_com);

                        },
                        4 =>{
                            single_str = (String::from("res:") +single_com);

                        },
                        _ => {}
                    }
                    // println!("{}",single_com);

                    single_vec.push(single_str);
                    count  = count + 1;
                }
                if if_broken{
                    continue;
                }
            }
            else if record[2].borrow().contains("NET"){
                let mut uid_tmp = "".to_string();
                let mut gid_tmp = "".to_string();
                for single_com in &record{
                    // let mut single_str = single_com.to_string();
                    let mut single_str = "".to_string();
                    if single_com.is_empty(){
                        if_broken = true;
                        break;
                    }
                    match count {
                        0 =>{
                            single_str = (String::from("UID:") +single_com);
                        },
                        1 =>{
                            single_str = (String::from("GID:") +single_com);

                        },
                        2 =>{
                            single_str = (String::from("logtype:") +single_com);
                        },
                        3 =>{
                            single_str = (String::from("op:") +single_com);
                        },
                        4 =>{
                            single_str = (String::from("res:") +single_com);
                        },
                        _ => {}
                    }
                    // println!("{}",single_com);
                    single_vec.push(single_str);
                    count  = count + 1;
                }
                if if_broken{
                    continue;
                }

            }
            transactions.push(single_vec);
        }

    }
    pb.finish_and_clear();
    Ok(transactions)
}







//load csv by shard
//input: csv file path ,loaded numbers ,
//output: original transaction and original parameter space


fn load_csv_by_shard(data_path:&str,loaded_num:u64)-> Result<(Vec<Vec<String>>,Vec<Vec<String>>,HashSet<String>)> {

    let mut transactions:Vec<Vec<String>> = Vec::new();
    let mut f = File::open(data_path).expect("expect a csv file");

    // 建立 CSV 读取器并且遍历每一条记录。
    // let mut rdr = csv::Reader::from_reader(f);
    // let mut reader = ReaderBuilder::new().delimiter(b'\t').from_reader(f);
    let mut reader = ReaderBuilder::new().delimiter(b',').from_reader(f);
   

    let mut unique_gid_uid_file:HashSet<(String,String)> = HashSet::new();
    let mut unique_op_file:HashSet<String> = HashSet::new();
    let mut unique_res_file:HashSet<String> = HashSet::new();
    let unique_type_file =vec!["logtype:FILE".to_string()];

    let mut unique_gid_uid_file_tmp:HashSet<(String,String)> = HashSet::new();
    let mut unique_op_file_tmp:HashSet<String> = HashSet::new();
    let mut unique_res_file_tmp:HashSet<String> = HashSet::new();
    

    let mut unique_op_net:HashSet<String> = HashSet::new();
    let mut unique_res_net:HashSet<String> = HashSet::new();
    let unique_type_net =vec!["logtype:NET".to_string()];
    let mut unique_gid_uid_net:HashSet<(String,String)> = HashSet::new();

    let mut unique_gid_uid_net_tmp:HashSet<(String,String)> = HashSet::new();
    let mut unique_op_net_tmp:HashSet<String> = HashSet::new();
    let mut unique_res_net_tmp:HashSet<String> = HashSet::new();
    let mut transactions_tmp:Vec<Vec<String>> = Vec::new();
    let mut uniqueSet:HashSet<String> = Default::default();
    let mut unique_set_tmp:HashSet<String> = Default::default();
    for n  in 1..21{
        let single_loaded_num = (loaded_num as f64 /20.0) as u64 ;
        let after_num = n * single_loaded_num;
        
        (transactions_tmp,unique_set_tmp,(unique_gid_uid_file_tmp,unique_res_file_tmp,unique_op_file_tmp,unique_gid_uid_net_tmp,unique_res_net_tmp,unique_op_net_tmp)) = load_csv_by_shard_loop(data_path, single_loaded_num,after_num).unwrap();
        transactions.append(&mut transactions_tmp.clone());
        
        for item in unique_gid_uid_file_tmp{
            unique_gid_uid_file.insert(item.clone());
        }
        for item in unique_gid_uid_net_tmp{
            unique_gid_uid_net.insert(item.clone());
        }
        for item in unique_op_file_tmp{
            unique_op_file.insert(item.clone());
        }
        for item in unique_op_net_tmp{
            unique_op_net.insert(item.clone());
        }
        for item in unique_res_file_tmp{
            unique_res_file.insert(item.clone());
        }
        for item in unique_res_net_tmp{
            unique_res_net.insert(item.clone());
        }
        
        for item in unique_set_tmp{
            uniqueSet.insert(item.clone());
        }
    }
    println!("set len={}",unique_gid_uid_file.len());
    println!("start creating para space");
    let mut para_space:Vec<Vec<String>> = Vec::new();
    //create file parameter space
    for (gid,uid) in &unique_gid_uid_file{
        for op in &unique_op_file{
            for res in &unique_res_file{
                for utype in &unique_type_file {
                    para_space.push(vec![uid.clone(), gid.clone(),utype.clone(),op.clone(),res.clone()]);
                }
            }
        }
    }

    //create net parameter space
    for (gid,uid) in &unique_gid_uid_net{
        for op in &unique_op_net{
            for res in &unique_res_net{
                for utype in &unique_type_net {
                    para_space.push(vec![uid.clone(), gid.clone(),utype.clone(),op.clone(),res.clone()]);
                }
            }
        }
    }


    Ok((transactions,para_space,uniqueSet))
}




fn load_csv_by_shard_loop(data_path:&str,loaded_num:u64,after_num:u64) -> Result<(Vec<Vec<String>>,HashSet<String>,(HashSet<(String,String)>,HashSet<String>,HashSet<String>,HashSet<(String,String)>,HashSet<String>,HashSet<String>))> {
    let mut transactions2 = Vec::new();
    let mut f = File::open(data_path).expect("expect a csv file");

    // 建立 CSV 读取器并且遍历每一条记录。
    // let mut rdr = csv::Reader::from_reader(f);
    // let mut reader = ReaderBuilder::new().delimiter(b'\t').from_reader(f);
    let mut reader = ReaderBuilder::new().delimiter(b',').from_reader(f);

    let mut if_broken = false;
    //create parameter space
    let mut para_space:Vec<Vec<String>> = Vec::new();
    let mut unique_uid_file:HashSet<String> = HashSet::new();
    let mut unique_gid_file:HashSet<String> = HashSet::new();
    let mut unique_gid_uid_file:HashSet<(String,String)> = HashSet::new();
    let mut unique_op_file:HashSet<String> = HashSet::new();
    let mut unique_res_file:HashSet<String> = HashSet::new();
    let unique_type_file =vec!["logtype:FILE".to_string()];

    
    let mut unique_uid_net:HashSet<String> = HashSet::new();
    let mut unique_gid_net:HashSet<String> = HashSet::new();
    let mut unique_op_net:HashSet<String> = HashSet::new();
    let mut unique_res_net:HashSet<String> = HashSet::new();
    let unique_type_net =vec!["logtype:NET".to_string()];
    let mut unique_gid_uid_net:HashSet<(String,String)> = HashSet::new();

    // let records_len = &reader.records().count();
    // println!("start coverting");
    let mut count = 0;
    let mut after_count = 0;

    for result in reader.records() {
        after_count = after_count + 1;
        if after_count < after_num {
            continue;
        }
        
        if count == loaded_num{
            break;
        }
        count = count + 1;

        // 遍历器会返回Result<StringRecord, Error>，所以我们在这里检查是否有错误。
        // let record = result.expect("is not 5");
        let mut record;
        match result {
            Ok(r)=>{record = r} ,
            Err(e)=>{
                continue
            }
        }
        // println!("{:?}",record);
        //clean data
        let mut single_vec = Vec::new();
        if !&record[2].contains("FILE")&&!&record[2].contains("NET") {
            println!("fail record:{:?}",&record);
            continue;
        }
        if_broken = false;

        if record.len() == 5 {
            let mut count = 0;
            if record[2].borrow().contains("FILE"){
                let mut uid_tmp = "".to_string();
                let mut gid_tmp = "".to_string();
                for single_com in &record{
                    // let mut single_str = single_com.to_string();

                    let mut single_str = "".to_string();
                    if single_com.is_empty(){
                        if_broken = true;
                        break;
                    }
                    match count {
                        0 =>{
                            single_str = (String::from("UID:") +single_com);
                            unique_uid_file.insert(single_str.clone());
                            uid_tmp = single_str.clone();
                        },
                        1 =>{
                            let mut hasher = DefaultHasher::new();
                            hasher.write(single_com.as_bytes());
                            let uid = hasher.finish() as i32;
                            single_str = String::from("GID:") +uid.to_string().as_str();
                            unique_gid_file.insert(single_str.clone());
                            gid_tmp = single_str.clone();
                        },
                        2 =>{
                            single_str = String::from("logtype:") +single_com;
                        },
                        3 =>{
                            single_str = (String::from("op:") +single_com);
                            unique_op_file.insert(single_str.clone());
                        },
                        4 =>{
                            single_str = (String::from("res:") +single_com);
                            unique_res_file.insert(single_str.clone());

                        },
                        _ => {}
                    }
                    // println!("{}",single_com);

                    single_vec.push(single_str);
                    count  = count + 1;
                }
                if if_broken{
                    continue;
                }
                unique_gid_uid_file.insert((gid_tmp.clone(),uid_tmp.clone()));
            }
            else if record[2].borrow().contains("NET"){
                let mut uid_tmp = "".to_string();
                let mut gid_tmp = "".to_string();
                for single_com in &record{
                    // let mut single_str = single_com.to_string();
                    let mut single_str = "".to_string();
                    if single_com.is_empty(){
                        if_broken = true;
                        break;
                    }
                    match count {
                        0 =>{
                            single_str = (String::from("UID:") +single_com);
                            unique_uid_net.insert(single_str.clone());
                            uid_tmp = single_str.clone();
                        },
                        1 =>{
                            single_str = (String::from("GID:") +single_com);
                            unique_gid_net.insert(single_str.clone());
                            gid_tmp = single_str.clone();
                        },
                        2 =>{
                            single_str = (String::from("logtype:") +single_com);
                        },
                        3 =>{
                            single_str = (String::from("op:") +single_com);
                            unique_op_net.insert(single_str.clone());

                        },
                        4 =>{
                            single_str = (String::from("res:") +single_com);
                            unique_res_net.insert(single_str.clone());
                        },
                        _ => {}
                    }
                    // println!("{}",single_com);
                    single_vec.push(single_str);
                    count  = count + 1;
                }
                if if_broken{
                    continue;
                }
                unique_gid_uid_net.insert((gid_tmp.clone(),uid_tmp.clone()));

            }
            transactions2.push(single_vec);
        }

    }
    // println!("start creating para space");
    //create file parameter space
    for (gid,uid) in &unique_gid_uid_file{
        for op in &unique_op_file{
            for res in &unique_res_file{
                for utype in &unique_type_file {
                    para_space.push(vec![uid.clone(), gid.clone(),utype.clone(),op.clone(),res.clone()]);
                }
            }
        }
    }

    //create net parameter space
    // for (gid,uid) in &unique_gid_uid_net{
    //     for op in &unique_op_net{
    //         for res in &unique_res_net{
    //             for utype in &unique_type_net {
    //                 para_space.push(vec![uid.clone(), gid.clone(),utype.clone(),op.clone(),res.clone()]);
    //             }
    //         }
    //     }
    // }
    //create unique HashSet
    let mut uniqueSet:HashSet<String> = Default::default();
    for gid in &unique_gid_file{
        uniqueSet.insert(gid.clone());
    }
    for gid in &unique_gid_net{
        uniqueSet.insert(gid.clone());
    }
    for uid in &unique_uid_file{
        uniqueSet.insert(uid.clone());
    }
    for uid in &unique_uid_net{
        uniqueSet.insert(uid.clone());
    }
    for res in &unique_res_file{
        uniqueSet.insert(res.clone());
    }
    for res in &unique_res_net{
        uniqueSet.insert(res.clone());
    }
    for op in &unique_op_file{
        uniqueSet.insert(op.clone());
    }
    for op in &unique_op_net{
        uniqueSet.insert(op.clone());
    }
    for utype in &unique_type_file{
        uniqueSet.insert(utype.clone());
    }
    for utype in &unique_type_net{
        uniqueSet.insert(utype.clone());
    }
    for utype in &unique_type_file{
        uniqueSet.insert(utype.clone());
    }
    for utype in &unique_type_net{
        uniqueSet.insert(utype.clone());
    }


    Ok((transactions2,uniqueSet,(unique_gid_uid_file,unique_res_file,unique_op_file,unique_gid_uid_net,unique_res_net,unique_op_net)))
}

//load csv to mongodb
//input: csv file path ,number of logs to load
//output: original transaction and original parameter space

async fn load_csv_to_mongodb(data_path:&str,loaded_num:u64) -> Result<(Vec<Vec<String>>,Vec<Vec<String>>,HashSet<String>)> {
    let pb = ProgressBar::new(loaded_num);
    pb.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len}({per_sec},{eta})")
        .unwrap()
        .progress_chars("#>-"));
    let client = Client::with_uri_str("mongodb://localhost").await.expect("failed to connect mongodb");
    let  (mut transactions,mut para_space,mut unique_set) = load_csv(data_path,loaded_num).expect("failed to load csv data");
    let transaction_original:Collection<Document> = client.database("esx_mining").collection("transacation_original");
    let transaction_original_distinct:Collection<Document> = client.database("esx_mining").collection("transacation_distinct");
    // transaction_original.delete_many(doc!{},None).await.expect("failed to delete existing data in mongodb");
    // transaction_original_distinct.delete_many(doc!{},None).await.expect("failed to delete existing data in mongodb");
    if transaction_original.count_documents(None,None).await.unwrap() != transactions.len().clone() as u64{
        transaction_original.delete_many(doc!{},None).await.expect("failed to delete existing data in mongodb");
        transaction_original_distinct.delete_many(doc!{},None).await.expect("failed to delete existing data in mongodb");
        let mut doc_set = Vec::new();
        let mut doc_set_distinct = Vec::new();
        // insert transaction to mongo
        for transaction in &transactions{
            pb.inc(1);
            let new_doc = doc! {
            "uid": transaction[0].clone(),
            "gid": transaction[1].clone(),
            "logtype": transaction[2].clone(),
            "op": transaction[3].clone(),
            "res":transaction[4].clone()
            };

            doc_set.push(new_doc.clone());
            if !doc_set_distinct.contains(new_doc.borrow()){
                doc_set_distinct.push(new_doc.clone());
            }
            if doc_set.len() >=1000 {
                transaction_original.insert_many(doc_set.clone(),None).await.expect("failed to insert data to mongodb");
                transaction_original_distinct.insert_many(doc_set_distinct.clone(),None).await.expect("failed to insert data to mongodb");
                doc_set.clear();
                doc_set_distinct.clear();
            }
        }
        transaction_original.insert_many(doc_set,None).await.expect("failed to insert data to mongodb");
        transaction_original_distinct.insert_many(doc_set_distinct.clone(),None).await.expect("failed to insert data to mongodb");
    }
    pb.finish_with_message("ending writing full data to mongo");
    println!("start writing para space.. ");
    let para_len = para_space.len().clone();

    let pb = ProgressBar::new(para_len as u64);
    pb.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len}({per_sec},{eta})")
        .unwrap()
        .progress_chars("#>-"));
    let para_space_original:Collection<Document> = client.database("esx_mining").collection("para_space_original");
    // para_space_original.delete_many(doc!{},None).await.expect("failed to delete existing data in mongodb");
    if para_space_original.count_documents(None,None).await.unwrap() != para_space.len().clone() as u64{
        para_space_original.delete_many(doc!{},None).await.expect("failed to delete existing data in mongodb");
        
        let mut doc_set = Vec::new();
        // insert transaction to mongo
        for transaction in &para_space{
            pb.inc(1);
            let new_doc = doc! {
            "uid": transaction[0].clone(),
            "gid": transaction[1].clone(),
            "logtype": transaction[2].clone(),
            "op": transaction[3].clone(),
            "res":transaction[4].clone()
        };
            doc_set.push(new_doc.clone());
            if doc_set.len() >=100000 {
                para_space_original.insert_many(doc_set.clone(),None).await.expect("failed to insert data to mongodb");
                doc_set.clear();
            }
        }
        para_space_original.insert_many(doc_set,None).await.expect("failed to insert data to mongodb");
    }
    pb.finish_with_message("ending writing full data to mongo");
    println!("len of transaction:{},len of para space:{}",transactions.len().clone(),para_space.len().clone());
    println!("len of mongo trans:{},len of mongo para:{}, len of distinct mongo trans:{}",transaction_original.count_documents(None,None).await.unwrap(),para_space_original.count_documents(None,None).await.unwrap(),transaction_original_distinct.count_documents(None,None).await.unwrap());






    Ok((transactions, para_space, unique_set))
}


//load csv to mongodb
//input: csv file path ,number of logs to load
//output: original transaction and original parameter space

async fn load_csv_to_mongodb_with_scoring(data_path:&str,loaded_num:u64,scoring_rate:f64,obp_rate:f64) -> Result<(Vec<Vec<String>>,HashSet<String>)> {
    let pb = ProgressBar::new(loaded_num);
    pb.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len}({per_sec},{eta})")
        .unwrap()
        .progress_chars("#>-"));
    let client = Client::with_uri_str("mongodb://localhost").await.expect("failed to connect mongodb");
    let transaction_original:Collection<Document> = client.database("esx_mining").collection("transacation_original");
    let transaction_original_distinct:Collection<Document> = client.database("esx_mining").collection("transacation_distinct");
    let transaction_original_backup:Collection<Document> = client.database("esx_mining").collection("transacation_original_backup");    
    let transaction_original_distinct_backup:Collection<Document> = client.database("esx_mining").collection("transacation_distinct_backup");
    // client.database("esx_mining").run_command(command, selection_criteria)
    let transaction_scoring:Collection<Document> = client.database("esx_mining").collection("transacation_scoring");
    let transaction_scoring_distinct:Collection<Document> = client.database("esx_mining").collection("transacation_scoring_distinct");
    let length_of_OBP = (loaded_num as f64 * (1.0-scoring_rate)) as usize;
    let mut transactions:Vec<Vec<String>> = Vec::new();
    let mut unique_set = HashSet::new();
    let mut transactions_to_output = Vec::new();
    // transaction_original_backup.delete_many(doc!{},None).await.expect("failed to delete existing data in mongodb");
    println!("back 里的数据量:{}",transaction_original_backup.count_documents(None,None).await.unwrap());
    // let  (mut transactions,mut para_space,mut unique_set) = load_csv(data_path,loaded_num).expect("failed to load csv data");
    if transaction_original_backup.count_documents(None,None).await.unwrap() == loaded_num as u64 {
        //不需要重新导入数据的情况
        let transaction_docs;
        let transactions_vecs;
        let length_of_OBP_true = (length_of_OBP as f64 * obp_rate) as usize;
        //start load mongodb
        (transactions_vecs,transaction_docs,unique_set) = load_mongodb_to_vec().await.expect("failed to load csv data");
        transactions_to_output = transactions_vecs[..length_of_OBP_true].to_vec();
        //start insert original
        transaction_original.delete_many(doc!{}, None).await.expect("failed to delete data in mongodb");
        transaction_original.insert_many(transaction_docs[..length_of_OBP_true].to_vec(), None).await.unwrap();
        
        //start insert scoring
        transaction_scoring.delete_many(doc!{}, None).await.expect("failed to delete data in mongodb");
        transaction_scoring.insert_many(transaction_docs[length_of_OBP..].to_vec(), None).await.expect("failed to delete data in mongodb");


        // 古法遍历去重，非常慢
        // let pb = ProgressBar::new(length_of_OBP.clone() as u64);
        // pb.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len}({per_sec},{eta})")
        //     .unwrap()
        //     .progress_chars("#>-"));
        // for single_transaction in transaction_docs[..length_of_OBP].to_vec(){
        //     pb.inc(1);
        //     if !transaction_docs_distinct_OBP.contains(&single_transaction){
        //         transaction_docs_distinct_OBP.push(single_transaction.clone());
        //     }
        // }
        // pb.finish_and_clear();
        // println!("distinct vec:{},transaction distinct mongo:{}",transaction_docs_distinct_OBP.len().clone(),transaction_original_distinct.estimated_document_count(None).await.unwrap());
        // println!("distinct vec:{},transaction distinct mongo:{}",transaction_docs_distinct_EXP.len().clone(),transaction_scoring_distinct.estimated_document_count(None).await.unwrap());
        

        
        let disk_use_opt = AggregateOptions::builder().allow_disk_use(true).build();
        //start to insert distinct of OBP
        transaction_original_distinct.delete_many(doc!{}, None).await.unwrap();
        let dedup_pipe_line = generate_distinct_logs("transacation_distinct");
        transaction_original.aggregate(dedup_pipe_line, disk_use_opt.clone()).await.expect("Failed to deduplicate");


        //start to insert distinct of EXP
        transaction_scoring_distinct.delete_many(doc!{}, None).await.unwrap();
        let dedup_pipe_line_scoring = generate_distinct_logs("transacation_scoring_distinct");
        transaction_scoring.aggregate(dedup_pipe_line_scoring, disk_use_opt.clone()).await.expect("Failed to deduplicate");

        

    }else {
        //需要重新导入数据的情况
        
        // transaction_original_backup.delete_many(doc!{},None).await.expect("failed to delete existing data in mongodb");
        // transaction_original_distinct_backup.delete_many(doc!{},None).await.expect("failed to delete existing data in mongodb");
        // (transactions, para_space,unique_set) = load_csv_by_shard(data_path,loaded_num).expect("failed to load csv data");
        (transactions, unique_set) = load_csv_with_para_and_backup_loaded(data_path, loaded_num, "transaction_backup").await.unwrap();
        println!("重新导入的数据长度为{}",transactions.len().clone());
        let transaction_whole_len = transactions.len().clone();
        if transaction_original.count_documents(None,None).await.unwrap() != transaction_whole_len.clone() as u64 {
            transaction_original.delete_many(doc!{},None).await.expect("failed to delete existing data in mongodb");
            transaction_original_distinct.delete_many(doc!{},None).await.expect("failed to delete existing data in mongodb");
            transaction_scoring.delete_many(doc!{},None).await.expect("failed to delete existing data in mongodb");
            transaction_scoring_distinct.delete_many(doc!{},None).await.expect("failed to delete existing data in mongodb");
            let mut doc_set = Vec::new();
            // let mut doc_set_distinct = Vec::new();
            let transfer_limit = 100000;
            let mut log_of_OBP_count:usize = 0;
            
            // insert transaction to mongo
            for transaction in &transactions{
                pb.inc(1);
                let new_doc = doc! {
                "uid": transaction[0].clone(),
                "gid": transaction[1].clone(),
                "logtype": transaction[2].clone(),
                "op": transaction[3].clone(),
                "res":transaction[4].clone()
                };
                doc_set.push(new_doc.clone());
                // if !doc_set_distinct.contains(new_doc.borrow()){
                //     doc_set_distinct.push(new_doc.clone());
                // }
                log_of_OBP_count = log_of_OBP_count + 1;
                if log_of_OBP_count < length_of_OBP {
                    if doc_set.len() >= transfer_limit {
                        transaction_original.insert_many(doc_set.clone(),None).await.expect("failed to insert data to mongodb");
                        // transaction_original_distinct.insert_many(doc_set_distinct.clone(),None).await.expect("failed to insert data to mongodb");
                        // transaction_original_backup.insert_many(doc_set.clone(),None).await.expect("failed to insert data to mongodb");
                        // transaction_original_distinct_backup.insert_many(doc_set_distinct.clone(),None).await.expect("failed to insert data to mongodb");

                        doc_set.clear();
                        // doc_set_distinct.clear();
                    }
                }else if log_of_OBP_count == length_of_OBP {
                    transaction_original.insert_many(doc_set.clone(),None).await.expect("failed to insert data to mongodb");
                    // transaction_original_distinct.insert_many(doc_set_distinct.clone(),None).await.expect("failed to insert data to mongodb");
                    // transaction_original_backup.insert_many(doc_set.clone(),None).await.expect("failed to insert data to mongodb");
                    // transaction_original_distinct_backup.insert_many(doc_set_distinct.clone(),None).await.expect("failed to insert data to mongodb");
                    doc_set.clear();
                    // doc_set_distinct.clear();
                }else {
                    if (doc_set.len() >= transfer_limit) || (log_of_OBP_count == transaction_whole_len.clone()) {
                        transaction_scoring.insert_many(doc_set.clone(),None).await.expect("failed to insert data to mongodb");
                        // transaction_scoring_distinct.insert_many(doc_set_distinct.clone(),None).await.expect("failed to insert data to mongodb");
                        // transaction_original_backup.insert_many(doc_set.clone(),None).await.expect("failed to insert data to mongodb");
                        // transaction_original_distinct_backup.insert_many(doc_set_distinct.clone(),None).await.expect("failed to insert data to mongodb");
                        doc_set.clear();
                        // doc_set_distinct.clear(); 
                    }
                }
            }
            let disk_use_opt = AggregateOptions::builder().allow_disk_use(true).build();

            //insert original distinct
            transaction_original.aggregate(generate_distinct_logs("transacation_distinct"),disk_use_opt.clone()).await.expect("failed to deduplicate");
            //insert backup distinct
            // transaction_original_backup.aggregate(generate_distinct_logs("transacation_distinct_backup"),None).await.expect("failed to deduplicate");
            //insert scoring distinct
            transaction_scoring.aggregate(generate_distinct_logs("transacation_scoring_distinct"),disk_use_opt.clone()).await.expect("failed to deduplicate");
            println!("trans backup :{},trans original:{},trans scoring:{}",
            transaction_original_distinct_backup.estimated_document_count(None).await?,
            transaction_original_distinct.estimated_document_count(None).await?,
            transaction_scoring_distinct.estimated_document_count(None).await?)


            // transaction_original.insert_many(doc_set,None).await.expect("failed to insert data to mongodb");
            // transaction_original_distinct.insert_many(doc_set_distinct.clone(),None).await.expect("failed to insert data to mongodb");
    }
        pb.finish_with_message("ending writing full data to mongo");
        // println!("start writing para space.. ");
        // println!("len of transaction:{},len of para space:{}",transactions.len().clone(),para_space.len().clone());
        // let para_space_original:Collection<Document> = client.database("esx_mining").collection("para_space_original");

        // println!("len of mongo trans:{},len of mongo para:{}, len of distinct mongo trans:{}",transaction_original.count_documents(None,None).await.unwrap(),para_space_original.count_documents(None,None).await.unwrap(),transaction_original_distinct.count_documents(None,None).await.unwrap());
        transactions_to_output = transactions[..length_of_OBP].to_vec();
    }

    

    // let transacation_obp =&transactions[..length_of_OBP]; 
    // let t_obp = transactions.clone_from_slice(transactions[..length_of_OBP.clone()].to_vec());
    Ok((transactions_to_output, unique_set))
}


async fn calculating_coverage_rate_mongo(frequent_pattern:&Vec<&str>) -> Result<(f64,usize,f64)>{
    let start_time = SystemTime::now();
    let client = Client::with_uri_str("mongodb://localhost").await.expect("failed to connect mongodb");
    let transaction_original:Collection<Document> = client.database("esx_mining").collection("transacation_original");
    let transaction_original_distinct:Collection<Document> = client.database("esx_mining").collection("transacation_distinct");
    let all_distinct_count = transaction_original_distinct.estimated_document_count(None).await.unwrap();
    let mut cover_logs_count = 0.0;
    let mut unique_cover_logs_count:usize =0;
    let mut transactions_count:f64 = 0.0;
    let mut coverage_rate = 0.0;
    let mut coverage_rate_distinct = 0.0;
    if frequent_pattern.contains(&"logtype:FILE") {
        let fsrule = generate_fsrule_from_item(&frequent_pattern).unwrap();

        let mut fs_rule_doc = fsrule.to_mongodb_doc();
        // println!("fs doc:{}",fs_rule_doc);
        cover_logs_count = transaction_original.count_documents(fs_rule_doc.clone(), None).await.unwrap() as f64;
        unique_cover_logs_count = transaction_original_distinct.count_documents(fs_rule_doc.clone(), None).await.unwrap() as usize;
        transactions_count = transaction_original.count_documents(None, None).await.unwrap() as f64;
    } else if frequent_pattern.contains(&"logtype:NET"){
        let netrule = generate_netrule_from_item(&frequent_pattern).unwrap();
        let mut net_rule_doc = netrule.to_mongodb_doc();
        cover_logs_count = transaction_original.count_documents(net_rule_doc.clone(), None).await.unwrap() as f64;
        unique_cover_logs_count = transaction_original_distinct.count_documents(net_rule_doc.clone(), None).await.unwrap() as usize;
        transactions_count = transaction_original.count_documents(None, None).await.unwrap() as f64;
    }
    if transactions_count!=0.0 && all_distinct_count!=0{
        coverage_rate = cover_logs_count as f64 / transactions_count as f64;
        coverage_rate_distinct = unique_cover_logs_count as f64 / all_distinct_count as f64;
    }
    let end_time = SystemTime::now().duration_since(start_time).unwrap().as_secs_f64();
    println!("mongo coverage calculation end, time use:{}s ,transcation_len = {},coverage_rate ={},cover_logs_count:{}",end_time,transactions_count,coverage_rate.clone(),cover_logs_count.clone());

    // println!("coverage Rate:{}",coverage_rate*0.5+coverage_rate_distinct*0.5);
    Ok((coverage_rate*0.5+coverage_rate_distinct*0.5, unique_cover_logs_count as usize, cover_logs_count as f64))
}


fn delete_logs_from_rule_normal(most_frequent_pattern:&Vec<&str>,transactions:&Vec<Vec<String>>)->Result<(Vec<Vec<String>>)>{
    let mut transacation_back:Vec<Vec<String>> = transactions.clone();
    if most_frequent_pattern.contains(&"logtype:FILE") {

            let fsrule = generate_fsrule_from_item(&most_frequent_pattern).unwrap();
            // println!("{}",&fsrule);
            let influence = fsrule.create_influence_vec();
            println!("influence: {:?}", &influence);
            //delete related logs
            // transaction_original.delete_many(fsrule.to_mongodb_doc(),None).await.expect("failed to delete existing data in mongodb");
            // transaction_original_distinct.delete_many(fsrule.to_mongodb_doc(),None).await.expect("failed to delete existing data in mongodb");

            let mut already_deleted: Vec<Vec<String>> = Vec::new();
            for single_item in transactions {
                let single_item_clone = single_item.clone();
                if already_deleted.contains(&single_item_clone) {
                    // println!("already have deleted");
                    continue;
                }
                // verify type
                if single_item.contains(&"logtype:FILE".to_string()) {
                    //verify GID
                    if fsrule.get_gid_string() == "GID:-1".to_string() || (single_item.contains(&fsrule.get_gid_string())) {
                        //verify UID
                        if fsrule.get_uid_string() == "UID:-1".to_string() || (single_item.contains(&fsrule.get_uid_string())) {
                            //verify op
                            if fsrule.get_op_string() == "op:any".to_string() || (single_item.contains(&fsrule.get_op_string())) {
                                //verify pathname
                                if single_item.contains(&fsrule.get_op_string()) {
                                    transacation_back.retain(|item| if *item == single_item_clone.clone() { false } else { true });
                                    already_deleted.push(single_item_clone.clone());
                                } else if fsrule.get_res_string().ends_with("*") {
                                    for item in single_item {
                                        if item.contains("res:") {
                                            if item.starts_with(fsrule.get_res_string().strip_suffix("*").unwrap()) || fsrule.get_res_string() =="res:/*" {
                                                transacation_back.retain(|item_str| if *item_str == single_item_clone.clone() { false } else { true });
                                                already_deleted.push(single_item_clone.clone());
                                                // println!("push prefix!")
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } else if most_frequent_pattern.contains(&"logtype:NET") {
            let netrule = generate_netrule_from_item(&most_frequent_pattern).unwrap();
            let influence = netrule.create_influence_vec();
            println!("influence: {:?}", &influence);

            //delete related logs
            // transaction_original.delete_many(netrule.to_mongodb_doc(),None).await.expect("failed to delete existing data in mongodb");
            // transaction_original_distinct.delete_many(netrule.to_mongodb_doc(),None).await.expect("failed to delete existing data in mongodb");

            let mut already_deleted: Vec<Vec<String>> = Vec::new();
            for single_item in transactions {
                let single_item_clone = single_item.clone();
                if already_deleted.contains(&single_item_clone) {
                    // println!("already have deleted");
                    continue;
                }
                if single_item.contains(&"logtype:NET".to_string()) {
                    //verify GID
                    if netrule.get_gid_string() == "GID:-1".to_string() || (single_item.contains(&netrule.get_gid_string())) {
                        //verify UID
                        if netrule.get_uid_string() == "UID:-1".to_string() || (single_item.contains(&netrule.get_uid_string())) {
                            //verify op
                            if netrule.get_op_string() == "op:any".to_string() || (single_item.contains(&netrule.get_op_string())) {
                                transacation_back.retain(|item| if *item == single_item_clone.clone() { false } else { true });
                                already_deleted.push(single_item_clone.clone());
                                println!("net push!");
                            }
                        }
                    }
                }
            }
        }
        Ok(transacation_back)
}


fn calculating_coverage_rate(transaction_original:&Vec<Vec<String>>,frequent_pattern:&Vec<&str>)-> Result<(f64,usize,f64)>{
    let start_time = SystemTime::now();
    let mut already_deleted: Vec<Vec<String>> = Vec::new();
    let mut unique_logs_count_t = 0;
    let mut influence_count:i64 = 0;
    let transaction_len = transaction_original.len();
    if frequent_pattern.contains(&"logtype:FILE"){
        let fsrule = generate_fsrule_from_item(&frequent_pattern).unwrap();
        println!("fs rule:{:?}",fsrule.create_influence_vec());
        // println!("{}",&fsrule);
        let influence = fsrule.create_influence_vec();
        let mut transacation_cp = transaction_original.clone();

        // while !&transacation_cp.is_empty(){
        //     let transaction_len_loop = transacation_cp.len().clone();
        //     let mut transacation_back = transacation_cp.clone();
        //     let mut count = 0;
        //     for single_item  in transacation_cp{
        //         let single_item_clone = single_item.clone();
        //         if single_item.contains(&"logtype:FILE".to_string()) {
        //             //verify GID,UID,op
        //             if (fsrule.get_gid_string() == "GID:-1".to_string() || (single_item.contains(&fsrule.get_gid_string())))
        //                 && (fsrule.get_uid_string() == "UID:-1".to_string() || (single_item.contains(&fsrule.get_uid_string())))
        //                 && (fsrule.get_op_string() == "op:any".to_string() || (single_item.contains(&fsrule.get_op_string()))) {
        //                 //verify pathname
        //                 if single_item.contains(&fsrule.get_op_string()) {
        //                     // influence_count = influence_count + 1;
        //                     // already_deleted.push(single_item_clone.clone());
        //                     unique_logs_count_t = unique_logs_count_t + 1;
        //                     transacation_back.retain(|item| if *item == single_item_clone.clone() {
        //                         influence_count = influence_count + 1;
        //                         false
        //                     } else { true });
        //
        //                 } else if fsrule.get_res_string().ends_with("*")  {
        //                     for item in single_item {
        //                         if item.contains("res:") {
        //                             if item.starts_with(fsrule.get_res_string().strip_suffix("*").unwrap()) || fsrule.get_res_string() == "/*".to_string() {
        //                                 unique_logs_count_t = unique_logs_count_t + 1;
        //                                 transacation_back.retain(|item| if *item == single_item_clone.clone() {
        //                                     influence_count = influence_count + 1;
        //                                     false
        //                                 } else { true });
        //                             }
        //                         }
        //                     }
        //                 }
        //             }
        //         }
        //         if transacation_back.len().clone() != transaction_len_loop.clone(){
        //             println!("data cp :{},data back:{}",transaction_len.clone(),transacation_back.len().clone());
        //             break;
        //         }
        //         count = count + 1;
        //     }
        //     // println!("exit");
        //     if(count == transaction_len_loop.clone()){
        //         break;
        //     }
        //     transacation_cp = transacation_back;
        //     // println!("trans cp:{}",transacation_cp.len().clone());
        // }




        for single_item in transaction_original{
            let single_item_clone = single_item.clone();
            if already_deleted.contains(&single_item_clone) {
                // println!("already have deleted");
                influence_count = influence_count + 1;
                continue;
            }
            // verify type
            if single_item.contains(&"logtype:FILE".to_string()) {
                //verify GID,UID,op
                if (fsrule.get_gid_string() == "GID:-1".to_string() || (single_item.contains(&fsrule.get_gid_string())))
                    && (fsrule.get_uid_string() == "UID:-1".to_string() || (single_item.contains(&fsrule.get_uid_string())))
                    && (fsrule.get_op_string() == "op:any".to_string() || (single_item.contains(&fsrule.get_op_string()))) {
                    //verify pathname
                    if single_item.contains(&fsrule.get_op_string()) {
                        influence_count = influence_count + 1;
                        already_deleted.push(single_item_clone.clone());
                    } else if fsrule.get_res_string().ends_with("*") {
                        for item in single_item {
                            if item.contains("res:") {
                                if item.starts_with(fsrule.get_res_string().strip_suffix("*").unwrap()) || fsrule.get_res_string() =="res:/*" {
                                    influence_count = influence_count + 1;
                                    already_deleted.push(single_item_clone.clone());
                                }
                            }
                        }
                    }
                }
            }
        }
    }else if frequent_pattern.contains(&"logtype:NET") {
        let netrule = generate_netrule_from_item(&frequent_pattern).unwrap();

        // println!("{}",&fsrule);
        let influence = netrule.create_influence_vec();
        for single_item in transaction_original {
            let single_item_clone = single_item.clone();
            // if already_deleted.contains(&single_item_clone) {
            //     // println!("already have deleted");
            //     influence_count = influence_count + 1;
            //     continue;
            // }
            if single_item.contains(&"logtype:NET".to_string()) {
                //verify GID,UID,op
                if (netrule.get_gid_string() == "GID:-1".to_string() || (single_item.contains(&netrule.get_gid_string())))
                    && (netrule.get_uid_string() == "UID:-1".to_string() || (single_item.contains(&netrule.get_uid_string())))
                    && (netrule.get_op_string() == "op:any".to_string() || (single_item.contains(&netrule.get_op_string()))) {
                    // influence_count = influence_count +1;
                    already_deleted.push(single_item_clone.clone());
                }
            }
        }
    }
    let end_time = SystemTime::now().duration_since(start_time).unwrap().as_secs_f64();

    let coverage_rate = influence_count as f64 /transaction_len as f64;
    let unique_rule_count = already_deleted.len();
    let cover_logs_count = influence_count as f64;
    // println!("coverage calculation end, time use:{}s ,transcation_len = {},coverage_rate ={},cover_logs_count:{}",end_time,&transaction_len,coverage_rate.clone(),cover_logs_count.clone());
    Ok((coverage_rate,unique_rule_count,cover_logs_count))
}
fn calculating_over_rate(transaction_original:&Vec<Vec<String>>,frequent_pattern:&Vec<&str>)-> Result<(f64,usize)>{
    let start_time = SystemTime::now();
    let mut influence_count:i64 = 0;
    let transaction_len = transaction_original.len();
    // let pb = ProgressBar::new(transaction_len.clone() as u64);
    // pb.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len}({per_sec})")
    //     .unwrap()
    //     .progress_chars("#>-"));

    if frequent_pattern.contains(&"logtype:FILE"){
        let fsrule = generate_fsrule_from_item(&frequent_pattern).unwrap();
        // println!("{}",&fsrule);
        for single_item in transaction_original{
            // pb.inc(1);
            // verify type
            if single_item.contains(&"logtype:FILE".to_string()) {
                //verify GID,UID,op
                if (fsrule.get_gid_string() == "GID:-1".to_string() || (single_item.contains(&fsrule.get_gid_string())))
                    && (fsrule.get_uid_string() == "UID:-1".to_string() || (single_item.contains(&fsrule.get_uid_string())))
                    && (fsrule.get_op_string() == "op:any".to_string() || (single_item.contains(&fsrule.get_op_string()))) {
                    //verify pathname
                    if single_item.contains(&fsrule.get_op_string()) {
                        influence_count = influence_count + 1;

                    } else if fsrule.get_res_string().ends_with("*") {
                        for item in single_item {
                            if item.contains("res:") {
                                if item.starts_with(fsrule.get_res_string().strip_suffix("*").unwrap()) {
                                    influence_count = influence_count + 1;
                                }
                            }
                        }
                    }
                }
            }
        }
    }else if frequent_pattern.contains(&"logtype:NET") {
        let netrule = generate_netrule_from_item(&frequent_pattern).unwrap();
        for single_item in transaction_original {
            // pb.inc(1);
            if single_item.contains(&"logtype:NET".to_string()) {
                //verify GID,UID,op
                if (netrule.get_gid_string() == "GID:-1".to_string() || (single_item.contains(&netrule.get_gid_string())))
                    && (netrule.get_uid_string() == "UID:-1".to_string() || (single_item.contains(&netrule.get_uid_string())))
                    && (netrule.get_op_string() == "op:any".to_string() || (single_item.contains(&netrule.get_op_string()))) {
                        influence_count = influence_count +1;
                }
            }
        }
    }

    // pb.finish_with_message("overate calculation done");
    // pb.finish_and_clear();
    let end_time = SystemTime::now().duration_since(start_time).unwrap().as_secs_f64();
    println!("overate calculation end, time use:{}s ,transcation_len = {}",end_time,&transaction_len);
    let coverage_rate = influence_count as f64 /transaction_len as f64;
    let unique_rule_count = influence_count as usize;
    Ok((coverage_rate,unique_rule_count))
}

async fn calculating_overate_rate_mongo(frequent_pattern:&Vec<&str>,para_space_len:f64) -> Result<(f64,usize)>{
    let start_time = SystemTime::now();
    let client = Client::with_uri_str("mongodb://localhost").await.expect("failed to connect mongodb");
    // let para_space:Collection<Document> = client.database("esx_mining").collection("para_space_original");
    let gid_collection:Collection<Document> = client.database("esx_mining").collection("para_space_gid");

    let mut cover_logs_count = 0.0;
    let mut unique_cover_logs_count:usize =0;
    let mut transactions_count:f64 = 0.0;
    let mut over_rate = 0.0;
    let mut query_doc = doc!{};
    let mut gid_created_by_pattern = "".to_string();

    // transactions_count = para_space.count_documents(None, None).await.unwrap() as f64;
    if frequent_pattern.contains(&"logtype:FILE") {
        let fsrule = generate_fsrule_from_item(&frequent_pattern).unwrap();
        query_doc = fsrule.to_mongodb_doc();
        gid_created_by_pattern = fsrule.get_gid_string();
        
    } else if frequent_pattern.contains(&"logtype:NET"){
        let netrule = generate_netrule_from_item(&frequent_pattern).unwrap();
        query_doc = netrule.to_mongodb_doc();
        gid_created_by_pattern = netrule.get_gid_string();
    }
    // gid_created_by_pattern = query_doc.get("gid").unwrap().to_string();

    // cover_logs_count = para_space.count_documents(query_doc.clone(), None).await.unwrap() as f64;

    let mut gids_cur = gid_collection.find(None, None).await.unwrap();
    let mut gids_vec:Vec<String> = Vec::new();
    let mut cover_logs_count2 = 0;
    let mut cover_logs_count3 = 0;
    // let mut para_space_len = 0;
    while let Some(gid_doc) = gids_cur.try_next().await? {
        // let doc: YearSummary = bson::from_document(single_movie?)?;
        // movie_vec.push(single_movie.clone());
        let gid = gid_doc.get("gid").unwrap().to_string().replace("\"", "");
        // let giddb:Collection<Document> = client.database("esx_mining").collection(("para_".to_owned()+gid.clone().as_str()).as_str());
        gids_vec.push(gid.clone());
        // println!("{:?}",gid_doc.get("gid").unwrap().to_string());
        // println!("found or* {}", single_year.get("_id").unwrap().as_document().unwrap().get("title").unwrap());
    }

    if gids_vec.contains(&gid_created_by_pattern){
        let start_time_find = SystemTime::now();
        //分表后，仅需要查询对应表

        let giddb:Collection<Document> = client.database("esx_mining").collection(("para_".to_owned()+gid_created_by_pattern.clone().as_str()).as_str());
        cover_logs_count2 = giddb.count_documents(query_doc.clone(), None).await.unwrap();
        let find_time = SystemTime::now().duration_since(start_time_find).unwrap().as_secs_f64();
        let find_time_t = SystemTime::now();
        // println!("collection:{} find end,time use:{},coverlogs_count3:{}",("para_".to_owned()+gid_created_by_pattern.clone().as_str()).as_str(),find_time,cover_logs_count2);
        // for gid in &gids_vec{
        //     let giddb:Collection<Document> = client.database("esx_mining").collection(("para_".to_owned()+gid.clone().as_str()).as_str());
        //     let para_tmp = giddb.estimated_document_count(None).await.unwrap();
        //     para_space_len = para_space_len + para_tmp;
        // }

        // println!("para len calculation time use:{}",SystemTime::now().duration_since(find_time_t).unwrap().as_secs_f64());




    } else{
        for gid in &gids_vec{
            let start_time_find = SystemTime::now();
            // println!("db name:{}",("para_".to_owned()+gid.clone().as_str()).as_str());
            let giddb:Collection<Document> = client.database("esx_mining").collection(("para_".to_owned()+gid.clone().as_str()).as_str());
            let cover_logs_tmp = giddb.count_documents(query_doc.clone(), None).await.unwrap();
            cover_logs_count2 = cover_logs_count2 + cover_logs_tmp;
            
            let find_time = SystemTime::now().duration_since(start_time_find).unwrap().as_secs_f64();
            let find_time_t = SystemTime::now();
            // println!("collection:{} find end,time use:{}",("para_".to_owned()+gid.clone().as_str()).as_str(),find_time);
        }
        let end_time = SystemTime::now().duration_since(start_time).unwrap().as_secs_f64();
        // let start_time2 = SystemTime::now();
        // let count_mutex_u64:Arc<Mutex<u64>> = Arc::new(Mutex::new(0 as u64));
        // for gid in &gids_vec{
        //     let start_time_find = SystemTime::now();
        //     // println!("db name:{}",("para_".to_owned()+gid.clone().as_str()).as_str());
        //     let giddb:Collection<Document> = client.database("esx_mining").collection(("para_".to_owned()+gid.clone().as_str()).as_str());
        //     let query_doc = query_doc.clone();
        //     let count_mutex_u64 = count_mutex_u64.clone();
        //     let handle = tokio::task::spawn(async move {get_cover_logs_from_collection(query_doc.clone(),  count_mutex_u64,giddb).await;});
        //     tokio::time::timeout(Duration::from_secs(100), handle).await??;
    
        // }
        // cover_logs_count3 = *count_mutex_u64.lock().unwrap();
        // let end_time2 = SystemTime::now().duration_since(start_time2).unwrap().as_secs_f64();
        // println!("over_rate time:{} s , cover_logs:{} , over_rate time:{} s,cover_logs2:{}",end_time,cover_logs_count2,end_time2,cover_logs_count3);
    }
    // let end_time = SystemTime::now().duration_since(start_time).unwrap().as_secs_f64();

    // let start_time2 = SystemTime::now();

    

    // let end_time2 = SystemTime::now().duration_since(start_time2).unwrap().as_secs_f64();

    

    
    // if transactions_count!=0.0{
    //     over_rate = cover_logs_count as f64 / transactions_count as f64;
    // }
    let mut over_rate2 = 0.0;
    if para_space_len !=0.0{
        over_rate2 = cover_logs_count2 as f64 / para_space_len as f64;
    }
    // println!("cover logs count1:{};count2:{},over_rate1:{};over_rate2:{}",cover_logs_count,cover_logs_count2,over_rate.clone(),over_rate2.clone());
    
    
    Ok((over_rate2,  cover_logs_count2 as usize))
}


async fn get_cover_logs_from_collection(query_doc:Document,mutex_count:mutex_u64,col:Collection<Document>){

    let count = col.count_documents(query_doc, None).await.unwrap();

    let mut mutex_logs_count = mutex_count.lock().unwrap();

    *mutex_logs_count += count;

}






async fn delete_logs_from_rule(frequent_pattern:&Vec<&str>)->Result<(Vec<Vec<String>>)>{
    // println!("start deleting");
    let start_time = SystemTime::now();
    let client = Client::with_uri_str("mongodb://localhost").await.expect("failed to connect mongodb");
    let transaction_original = client.database("esx_mining").collection::<EsxLog>("transacation_original");
    let transaction_original_distinct = client.database("esx_mining").collection::<EsxLog>("transacation_distinct");
    println!("trans len = {}",transaction_original.count_documents(None,None).await.unwrap());
    let mut delete_doc=doc!{};
    if frequent_pattern.contains(&"logtype:FILE") {
        let fsrule = generate_fsrule_from_item(&frequent_pattern).unwrap();
        delete_doc =fsrule.to_mongodb_doc();
    }else if frequent_pattern.contains(&"logtype:NET") {
        let netrule = generate_netrule_from_item(&frequent_pattern).unwrap();
        delete_doc =netrule.to_mongodb_doc();
    }
    transaction_original.delete_many(delete_doc.clone(),None).await.expect("failed to delete existing data in mongodb");
    transaction_original_distinct.delete_many(delete_doc.clone(),None).await.expect("failed to delete existing data in mongodb");
    
    
    let mut transaction_collection = transaction_original.find(None,None).await.unwrap();
    let mut transcation_new:Vec<Vec<String>> = Vec::new();

    // Loop through the results and print a summary and the comments:
    while let Some(single_log) = transaction_collection.try_next().await? {
        // let doc: EsxLog = bson::from_document(single_log)?;
        transcation_new.push(single_log.to_logs_vec());
        // println!("* {:?}", single_log.to_logs_vec());
    }

    println!("trans new len:{}",transcation_new.len());
    let end_time = SystemTime::now().duration_since(start_time).unwrap().as_secs_f64();
    println!(" single delete mongo time use:{}",end_time);
    


    Ok(transcation_new)
}


async fn evaluation_policy(frequent_pattern_set:&Vec<Vec<String>>)->Result<(f64,f64,f64,f64,f64)>{
    let start_time = SystemTime::now();
    let client = Client::with_uri_str("mongodb://localhost").await.expect("failed to connect mongodb");
    let transaction_scoring:Collection<Document> = client.database("esx_mining").collection("transacation_scoring");
    let transaction_scoring_distinct:Collection<Document> = client.database("esx_mining").collection("transacation_scoring_distinct");
    // let para_space_original:Collection<Document> = client.database("esx_mining").collection("para_space_original");
    let gid_collection:Collection<Document> = client.database("esx_mining").collection("para_space_gid");
    let mut policy_doc = Vec::new();
    for single_item in frequent_pattern_set{
        if single_item.contains(&"logtype:FILE".to_string()){
            let fs_rule_doc = FilesystemRule::from_pattern_String(&single_item.clone()).to_mongodb_doc();
            policy_doc.push(fs_rule_doc);
        }else if single_item.contains(&"logtype:NET".to_string()){
            let net_rule_doc = NetRule::from_pattern_String(&single_item.clone()).to_mongodb_doc();
            policy_doc.push(net_rule_doc);
        }
    }
    
    let policy_doc_for_mongo = doc! {
        "$or":policy_doc
    };
    let policy_cover_count = transaction_scoring.count_documents(policy_doc_for_mongo.clone(),None).await.expect("failed to connect to mongo");
    let policy_cover_count_distinct = transaction_scoring_distinct.count_documents(policy_doc_for_mongo.clone(),None).await.expect("failed to connect to mongo");
    
    let transaction_len_of_OPP = transaction_scoring.count_documents(None,None).await.expect("failed to connect to mongo");
    let transaction_len_of_OPP_distinct = transaction_scoring_distinct.count_documents(None,None).await.expect("failed to connect to mongo");




    // let policy_cover_para_space_count = para_space_original.count_documents(policy_doc_for_mongo.clone(),None).await.expect("failed to connect to mongo");
    // let para_space_size:u64 = para_space_original.count_documents(None,None).await.expect("failed to connect to mongo");

    let mut gids_cur = gid_collection.find(None, None).await.unwrap();
    let mut gids_vec:Vec<String> = Vec::new();
    let mut policy_cover_para_space_count = 0;
    let mut para_space_size:u64 = 0;
    while let Some(gid_doc) = gids_cur.try_next().await? {
        let gid = gid_doc.get("gid").unwrap().to_string().replace("\"", "");
        gids_vec.push(gid.clone());
    }
    for gid in gids_vec{
        println!("db name:{}",("para_".to_owned()+gid.clone().as_str()).as_str());
        let giddb:Collection<Document> = client.database("esx_mining").collection(("para_".to_owned()+gid.clone().as_str()).as_str());
        let cover_logs_tmp = giddb.count_documents(policy_doc_for_mongo.clone(), None).await.unwrap();
        policy_cover_para_space_count = policy_cover_para_space_count + cover_logs_tmp;
        let para_tmp = giddb.estimated_document_count(None).await.unwrap();
        para_space_size = para_space_size + para_tmp;
    }
    


    // let TruePositiveNum:u64 = policy_cover_count.clone();
    let TruePositiveNum:u64 = policy_cover_count_distinct.clone();
    let TruePositiveNum_no_distinct:u64 = policy_cover_count.clone();
    
    let FalseNegativeNum:u64 = transaction_len_of_OPP_distinct - policy_cover_count_distinct;
    let FalseNegativeNum_no_distinct:u64 = transaction_len_of_OPP - policy_cover_count;
    let FalsePositiveNum:u64 = policy_cover_para_space_count - policy_cover_count_distinct;
    
    
    
    let TrueNegativeNum:u64 = para_space_size - (TruePositiveNum+FalseNegativeNum+FalsePositiveNum);
    println!("TP:{},FN:{},FP:{},TN:{}",TruePositiveNum.clone(),FalseNegativeNum.clone(),FalsePositiveNum.clone(),TrueNegativeNum.clone());
    let mut TrueNegativeNum_no_distinct:i64 = para_space_size as i64 - (TruePositiveNum_no_distinct+FalseNegativeNum_no_distinct+FalsePositiveNum) as i64;
    if TrueNegativeNum_no_distinct < 0{
        TrueNegativeNum_no_distinct = 0;
    }
    
    let mut TPR = 0.0;
    let mut TPR_no_distinct = 0.0;
    if TruePositiveNum+FalseNegativeNum == 0{
        TPR = 1.0;
    }
    else{
        TPR = (TruePositiveNum as f64)/(TruePositiveNum+FalseNegativeNum) as f64;
    }
    let FPR = (FalsePositiveNum as f64)/(FalsePositiveNum + TrueNegativeNum)as f64;
    let precision = (TruePositiveNum as f64)/(TruePositiveNum+FalsePositiveNum) as f64;
    if TruePositiveNum_no_distinct+FalseNegativeNum_no_distinct == 0{
        TPR_no_distinct = 1.0;
    }
    else{
        TPR_no_distinct = (TruePositiveNum_no_distinct as f64)/(TruePositiveNum_no_distinct+FalseNegativeNum_no_distinct) as f64;
    }
    let FPR_no_distinct = (FalsePositiveNum as f64)/((FalsePositiveNum as u64 + TrueNegativeNum_no_distinct as u64)as f64);
    println!("fpn:{}, tnnnd:{}",FalsePositiveNum as f64,TrueNegativeNum_no_distinct as u64 as f64);
    let precision = (TruePositiveNum as f64)/(TruePositiveNum_no_distinct+FalsePositiveNum) as f64;

    Ok((precision,TPR,FPR,TPR_no_distinct,FPR_no_distinct))
}

async fn load_mongodb_to_vec() ->Result<(Vec<Vec<String>>,Vec<Document>,HashSet<String>)>{
    let client = Client::with_uri_str("mongodb://localhost").await.expect("failed to connect mongodb");
    let transaction_original_backup = client.database("esx_mining").collection::<EsxLog>("transacation_original_backup");
    let transaction_original_distinct = client.database("esx_mining").collection::<EsxLog>("transacation_distinct_backup");

    let mut transaction_collection = transaction_original_backup.find(None,None).await.expect("failed to connect to backup");
    // println!("back 里的数据量:{}",transaction_original_backup.count_documents(None,None).await.unwrap());
    let backup_len = transaction_original_backup.estimated_document_count(None).await.unwrap();
    let mut transcation_new:Vec<Vec<String>> = Vec::new();
    let mut uniqueSet:HashSet<String> = Default::default();
    let mut transaction_docs:Vec<Document> = Vec::new();
    let mut transaction_docs_distinct:Vec<Document> = Vec::new();
    // Loop through the results and print a summary and the comments:
    let pb = ProgressBar::new(backup_len);
    pb.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len}({per_sec},{eta})")
        .unwrap()
        .progress_chars("#>-"));
    while let Some(single_esx_log) = transaction_collection.try_next().await.expect("failed to change document to EsxLog") {
        pb.inc(1);
        // let single_esx_log: EsxLog = bson::from_document(single_log.clone()).expect("failed to change document to EsxLog");
        transcation_new.push(single_esx_log.to_logs_vec());
        let doc_tmp = bson::to_document(&single_esx_log).unwrap();
        transaction_docs.push(doc_tmp.clone());
        // println!("* {:?}", single_log.to_logs_vec());
    }
    pb.finish_and_clear();
    // println!("len of tran new:{}",transcation_new.len().clone());
    // let mut transaction_collection = transaction_original_distinct.find(None,None).await.unwrap();
    // while let Some(single_log) = transaction_collection.try_next().await? {
    //     // let doc: EsxLog = bson::from_document(single_log)?;
        
    //     let doc_tmp = bson::to_document(&single_log).unwrap();
    //     transaction_docs_distinct.push(doc_tmp.clone());
        
    //     // println!("* {:?}", single_log.to_logs_vec());
    // }
    // let para_space_original = client.database("esx_mining").collection::<EsxLog>("para_space_original");

    // let mut transaction_collection = para_space_original.find(None,None).await.unwrap();
    // let mut transcation_para:Vec<Vec<String>> = Vec::new();

    // // Loop through the results and print a summary and the comments:
    // while let Some(single_log) = transaction_collection.try_next().await? {
    //     // let doc: EsxLog = bson::from_document(single_log)?;
    //     transcation_para.push(single_log.to_logs_vec());
        
    //     // println!("* {:?}", single_log.to_logs_vec());
    // }
    //create unique HashSet



    Ok((transcation_new,transaction_docs,uniqueSet))
}