#include "data_loader.hpp"
#include "gbdt_tree.hpp"
#include "math_tools.hpp"
#include "util.hpp"

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "driver/engine.hpp"
#include "worker/kv_client_table.hpp"

#include <limits>
#include <map>
#include <string>
#include <vector>

namespace flexps {

GBDTTree::GBDTTree(std::map<std::string, float> & params) {
  this->left_child = NULL;
  this->right_child = NULL;
  this->feat_id = -1;
  this->split_val = -1;
  this->max_gain = -std::numeric_limits<float>::infinity();
  this->depth = 0;
  this->predict_val = 0.0;
  this->is_leaf = false;
  this->params = params;
}

void GBDTTree::train(int & ps_key_ptr, std::unique_ptr<KVClientTable<float>> & table, std::vector<std::vector<float>> feat_vect_list, std::vector<std::map<std::string, float>> min_max_feat_list
	,std::vector<float> grad_vect, std::vector<float> hess_vect) {
  std::vector<Key> push_key_vect;
  std::vector<float> push_val_vect;

  // Step 1: Calculate and push quantile sketch
  // Space needed on PS: (1 / (0.1 * rank_fraction)) * num_of_feat
  //LOG(INFO) << "Step 1: Calculate and push quantile sketch";

  std::vector<Key> aggr_push_key_vect;
  std::vector<float> aggr_push_val_vect;

  for (int f_id = 0; f_id < feat_vect_list.size(); f_id++) {
    push_key_vect = push_quantile_sketch(ps_key_ptr, table, feat_vect_list[f_id], min_max_feat_list[f_id], push_val_vect);
    this->quantile_sketch_key_vect_list.push_back(push_key_vect);
    this->quantile_sketch_val_vect_list.push_back(push_val_vect);
  	aggr_push_key_vect.insert(aggr_push_key_vect.end(), push_key_vect.begin(), push_key_vect.end());
  	aggr_push_val_vect.insert(aggr_push_val_vect.end(), push_val_vect.begin(), push_val_vect.end());
  	
    //LOG(INFO) << "aggr_push_val_vect.sum = " << std::accumulate(aggr_push_val_vect.begin(), aggr_push_val_vect.end(), 0.0);
  }
  table->Add(aggr_push_key_vect, aggr_push_val_vect);
  table->Clock();

  
  // Step 2: Pull global quantile sketch result and find split candidates
  //LOG(INFO) << "Step 2: Pull global quantile sketch result and find split candidates";

  std::vector<Key> pull_key_vect;
  std::vector<float> pull_val_vect;

  for (int f_id = 0; f_id < this->quantile_sketch_key_vect_list.size(); f_id++) {
  	std::vector<Key> key_vect = this->quantile_sketch_key_vect_list[f_id];
  	pull_key_vect.insert(pull_key_vect.end(), key_vect.begin(), key_vect.end());
  	
  }
  table->Get(pull_key_vect, &pull_val_vect);
  // Check log
  /*
  for (int i = 0; i < pull_val_vect.size(); i++) {
  	LOG(INFO) << "pull_key_vect[" << i << "] = " << pull_key_vect[i];
  	LOG(INFO) << "pull_val_vect[" << i << "] = " << pull_val_vect[i];
  	
  }
  */
  std::vector<std::vector<float>> candidate_split_vect_list;
  int sketch_num_per_feat = pull_val_vect.size() / feat_vect_list.size(); 
  for (int f_id = 0; f_id < feat_vect_list.size(); f_id++) {
    std::vector<float> sketch_hist_vect(pull_val_vect.begin() + (f_id * sketch_num_per_feat), pull_val_vect.begin() + ((f_id + 1) * sketch_num_per_feat));
    std::vector<float> candidate_split_vect = find_candidate_split(sketch_hist_vect, min_max_feat_list[f_id]);
    /*
    for (float val: candidate_split_vect) {
    	LOG(INFO) << "candidate: " << val;
    }
    */
    candidate_split_vect_list.push_back(candidate_split_vect);
  }

  
  // Step 3: Calculate local grad and hess for each candidate and push the result to ps
  // Space needed on PS: ((1 / rank_fraction) - 1) * 4 * num_of_feat
  //LOG(INFO) << "Step 3: Calculate local grad and hess for each candidate and push the result to ps";

  aggr_push_key_vect.clear();
  aggr_push_val_vect.clear();
  //LOG(INFO) << "After clear vect, aggr_push_key_vect.size = " << aggr_push_key_vect.size() << ", aggr_push_val_vect.size = " << aggr_push_val_vect.size();
  for (int f_id = 0; f_id < feat_vect_list.size(); f_id++) {
  	// Ordering of push_key_vect:
  	// left grad, left hess, right grad, right hess
  	push_key_vect = push_local_grad_hess(ps_key_ptr, table, feat_vect_list[f_id], candidate_split_vect_list[f_id], grad_vect, hess_vect, push_val_vect);
  	this->grad_hess_key_vect_list.push_back(push_key_vect);
    this->grad_hess_val_vect_list.push_back(push_val_vect);
  	aggr_push_key_vect.insert(aggr_push_key_vect.end(), push_key_vect.begin(), push_key_vect.end());
  	aggr_push_val_vect.insert(aggr_push_val_vect.end(), push_val_vect.begin(), push_val_vect.end());
  }
  //LOG(INFO) << "ps_key_ptr = " << ps_key_ptr;
  table->Add(aggr_push_key_vect, aggr_push_val_vect);
  table->Clock();


  // Step 4: Pull global grad and hess and find the best split
  //LOG(INFO) << "Step 4: Pull global grad and hess and find the best split";

  pull_key_vect.clear();
  pull_val_vect.clear();

  for (int f_id = 0; f_id < this->grad_hess_key_vect_list.size(); f_id++) {
  	std::vector<Key> key_vect = this->grad_hess_key_vect_list[f_id];
  	pull_key_vect.insert(pull_key_vect.end(), key_vect.begin(), key_vect.end());
  	
  }
  table->Get(pull_key_vect, &pull_val_vect);

  int grad_hess_num_per_feat = ((1 / this->params["rank_fraction"]) - 1) * 4;
  for (int f_id = 0; f_id < feat_vect_list.size(); f_id++) {
    std::vector<float> grad_hess_vect(pull_val_vect.begin() + (f_id * grad_hess_num_per_feat), pull_val_vect.begin() + ((f_id + 1) * grad_hess_num_per_feat));
    std::map<std::string, float> best_split = find_best_split(grad_hess_vect, this->params["complexity_of_leaf"]);
    
    // Update best split info for this node
    if (best_split["best_split_gain"] > this->max_gain) {
      this->max_gain = best_split["best_split_gain"];
      this->feat_id = f_id;
      this->split_val = candidate_split_vect_list[f_id][best_split["best_candidate_id"]];
    }
  }
  //LOG(INFO) << "this->max_gain = " << this->max_gain;
  //LOG(INFO) << "this->feat_id = " << this->feat_id;
  //LOG(INFO) << "this->split_val = " << this->split_val;


  // Step 5: Reset ps val for next node use
  //LOG(INFO) << "Step 5: Reset ps val for next node use";

  aggr_push_key_vect.clear();
  aggr_push_val_vect.clear();
  for (int i = 0; i < this->quantile_sketch_key_vect_list.size(); i++) {
    // TODO: dont use transform
    std::vector<float> inv_val_vect;
    for (int j = 0; j < this->quantile_sketch_key_vect_list[i].size(); j++) {
      inv_val_vect.push_back(this->quantile_sketch_val_vect_list[i][j] * -1.0);
    }
    push_val_vect = inv_val_vect;
    //push_val_vect = std::transform(this->quantile_sketch_val_vect_list[i].begin(), this->quantile_sketch_val_vect_list[i].end(), this->quantile_sketch_val_vect_list[i].begin(), std::bind1st(std::multiplies<float>(), -1.0));
    aggr_push_key_vect.insert(aggr_push_key_vect.end(), quantile_sketch_key_vect_list[i].begin(), quantile_sketch_key_vect_list[i].end());
    aggr_push_val_vect.insert(aggr_push_val_vect.end(), push_val_vect.begin(), push_val_vect.end());
  }
  //LOG(INFO) << "aggr_push_key_vect.size = " << aggr_push_key_vect.size();
  //LOG(INFO) << "aggr_push_val_vect.size = " << aggr_push_val_vect.size();

  for (int i = 0; i < this->grad_hess_key_vect_list.size(); i++) {
    std::vector<float> inv_val_vect;
    for (int j = 0; j < this->grad_hess_key_vect_list[i].size(); j++) {
      inv_val_vect.push_back(this->grad_hess_val_vect_list[i][j] * -1.0);
    }
    push_val_vect = inv_val_vect;
    //push_val_vect = std::transform(this->grad_hess_val_vect_list[i].begin(), this->grad_hess_val_vect_list[i].end(), this->grad_hess_val_vect_list[i].begin(), std::bind1st(std::multiplies<float>(), -1.0));
    aggr_push_key_vect.insert(aggr_push_key_vect.end(), grad_hess_key_vect_list[i].begin(), grad_hess_key_vect_list[i].end());
    aggr_push_val_vect.insert(aggr_push_val_vect.end(), push_val_vect.begin(), push_val_vect.end());
  }
  //LOG(INFO) << "aggr_push_key_vect.size = " << aggr_push_key_vect.size();
  //LOG(INFO) << "aggr_push_val_vect.size = " << aggr_push_val_vect.size();
  
  table->Add(aggr_push_key_vect, aggr_push_val_vect);

  // Reset ps_key_ptr
  ps_key_ptr = 0;

  table->Clock();
  // Check log
  /*
  pull_val_vect.clear();
  table->Get(aggr_push_key_vect, &pull_val_vect);
  //LOG(INFO) << "sum(pull_val_vect) (should be 0) = " << std::accumulate(pull_val_vect.begin(), pull_val_vect.end(), 0.0);
  */

  // Step 6: Check to stop
  //LOG(INFO) << "Step 6: Check to stop";
  if (check_to_stop()) {
    this->predict_val = std::accumulate(grad_vect.begin(), grad_vect.end(), 0.0) / grad_vect.size();
    this->is_leaf = true;
    // TODO: mark the clock?

    return;
  }

  // Step 7: Recursively build child
  //LOG(INFO) << "Step 7: Recursively build child";
  std::vector<float> left_grad_vect, right_grad_vect;
  std::vector<float> left_hess_vect, right_hess_vect;
  std::vector<std::vector<float>> left_feat_vect_list, right_feat_vect_list;
  DataLoader::split_dataset_by_feat_val(
    this->feat_id,
    this->split_val,
    grad_vect,
    left_grad_vect,
    right_grad_vect,
    hess_vect,
    left_hess_vect,
    right_hess_vect,
    feat_vect_list,
    left_feat_vect_list,
    right_feat_vect_list
  );
  /*
  LOG(INFO) << "left_grad_vect.size = " << left_grad_vect.size();
  LOG(INFO) << "right_grad_vect.size = " << right_grad_vect.size();
  LOG(INFO) << "left_hess_vect.size = " << left_hess_vect.size();
  LOG(INFO) << "right_hess_vect.size = " << right_hess_vect.size();
  LOG(INFO) << "left_feat_vect_list.size = " << left_feat_vect_list.size();
  LOG(INFO) << "right_feat_vect_list.size = " << right_feat_vect_list.size();
  LOG(INFO) << "left_feat_vect_list[0].size = " << left_feat_vect_list[0].size();
  LOG(INFO) << "right_feat_vect_list[0].size = " << right_feat_vect_list[0].size();
  */
  this->left_child = new GBDTTree(this->params);
  this->right_child = new GBDTTree(this->params);
  this->left_child->set_depth(this->depth + 1);
  this->right_child->set_depth(this->depth + 1);

  this->left_child->train(ps_key_ptr, table, left_feat_vect_list, min_max_feat_list
  , left_grad_vect, left_hess_vect);
  this->right_child->train(ps_key_ptr, table, right_feat_vect_list, min_max_feat_list
  , right_grad_vect, right_hess_vect);

}

std::vector<Key> GBDTTree::push_quantile_sketch(int & ps_key_ptr, std::unique_ptr<KVClientTable<float>> & table, std::vector<float> feat_vect, std::map<std::string, float> min_max_feat, std::vector<float> & _push_val_vect) {
  float rank_fraction = this->params["rank_fraction"];
  float total_data_num = this->params["total_data_num"];
  float min = min_max_feat["min"];
  float max = min_max_feat["max"];
  max += 1; // To include largest element

  // Step 1: Create histogram
  //float rank_bin_width = rank_fraction * total_data_num;
  int rank_num = (int) 1.0 / rank_fraction;
  std::vector<float> rank_fraction_vect;
  for (int i = 1; i < rank_num; i++) {
  	rank_fraction_vect.push_back(rank_fraction * i);
  }

  float sketch_fraction = 0.1 * rank_fraction;
  int sketch_num = (int) 1.0 / sketch_fraction;
  std::vector<float> sketch_bin_vect;
  for (int i = 0; i <= sketch_num; i++) {
  	sketch_bin_vect.push_back(sketch_fraction * i);
  }

  // Step 2: Accumulate histogram
  std::vector<float> sketch_hist_vect(sketch_num, 0.0);
  for(int i = 0; i < feat_vect.size(); i++) {
  	float weight_val = (feat_vect[i] - min) / (max - min);
  	for(int j = 0; j < sketch_bin_vect.size() - 1; j ++) {
  	  if (sketch_bin_vect[j] <= weight_val && weight_val < sketch_bin_vect[j + 1]) {
  	    sketch_hist_vect[j] += 1;
  	    break;
  	  }
  	}
  }

  std::vector<float> push_val_vect = sketch_hist_vect;
  std::vector<Key> push_key_vect(push_val_vect.size());
  std::iota(push_key_vect.begin(), push_key_vect.end(), ps_key_ptr);
  ps_key_ptr += push_key_vect.size();

  _push_val_vect = push_val_vect;
  return push_key_vect;
}

std::vector<float> GBDTTree::find_candidate_split(std::vector<float> sketch_hist_vect, std::map<std::string, float> min_max_feat) {
  float rank_fraction = this->params["rank_fraction"];
  float total_data_num = this->params["total_data_num"];
  float min = min_max_feat["min"];
  float max = min_max_feat["max"];
  max += 1; // To include largest element
  /*
  for (int i = 0; i < sketch_hist_vect.size(); i++) {
  	LOG(INFO) << "sketch_hist_vect[" << i << "] = " << sketch_hist_vect[i];
  }
  LOG(INFO) << "sketch_hist_vect.sum" << std::accumulate(sketch_hist_vect.begin(), sketch_hist_vect.end(), 0.0);
  */

  int rank_num = (int) 1.0 / rank_fraction;
  std::vector<float> rank_fraction_vect;
  for (int i = 1; i < rank_num; i++) {
  	rank_fraction_vect.push_back(rank_fraction * i);
  }
  float rank_bin_width = rank_fraction * total_data_num;

  float sketch_fraction = 0.1 * rank_fraction;
  int sketch_num = (int) 1.0 / sketch_fraction;

  int sketch_candidate_range = 2 * sketch_num / rank_num;

  std::vector<float> candidate_vect;
  float prev_fraction = 0.0;
  for (int c_id = 0; c_id < rank_fraction_vect.size(); c_id++) {
    float argmax_fraction = prev_fraction;
    float best_approx = std::numeric_limits<float>::infinity();

    float cur_candidate_aggr_num = (c_id + 1) * rank_bin_width;
    for (int s_id = 0; s_id < sketch_candidate_range; s_id++) {
      float cur_fraction = prev_fraction + s_id * sketch_fraction;
      float cur_fraction_sum = std::accumulate(sketch_hist_vect.begin(), sketch_hist_vect.begin() + (int)(cur_fraction / sketch_fraction), 0.0);

      if (fabs(cur_fraction_sum - cur_candidate_aggr_num) < best_approx) {
        best_approx = fabs(cur_fraction_sum - cur_candidate_aggr_num);
        argmax_fraction = cur_fraction;
      }
    }
    prev_fraction = argmax_fraction;
    candidate_vect.push_back(argmax_fraction * (max - min) + min);
  }
  return candidate_vect;
}

std::vector<Key> GBDTTree::push_local_grad_hess(int & ps_key_ptr, std::unique_ptr<KVClientTable<float>> & table, std::vector<float> feat_vect, std::vector<float> candidate_split_vect
	,std::vector<float> grad_vect, std::vector<float> hess_vect, std::vector<float> & _push_val_vect) {
  int candidate_num_per_feat = candidate_split_vect.size();
  //LOG(INFO) << "candidate_num_per_feat = " << candidate_num_per_feat;
  std::vector<float> left_grad_val_vect(candidate_num_per_feat, 0.0);
  std::vector<float> left_hess_val_vect(candidate_num_per_feat, 0.0);
  std::vector<float> right_grad_val_vect(candidate_num_per_feat, 0.0);
  std::vector<float> right_hess_val_vect(candidate_num_per_feat, 0.0);

  // Step 1: Accumulate left and right grad/hess val vect
  //LOG(INFO) << "feat_vect.size = " << feat_vect.size();
  //LOG(INFO) << "grad_vect.size = " << grad_vect.size();
  //LOG(INFO) << "hess_vect.size = " << hess_vect.size();
  for (int col_id = 0; col_id < feat_vect.size(); col_id++) {
    float feat_val = feat_vect[col_id];
    for (int c_id = 0; c_id < candidate_num_per_feat; c_id++) {
      float candidate_split_val = candidate_split_vect[c_id];

      if (feat_val < candidate_split_val) {
        left_grad_val_vect[c_id] += grad_vect[col_id];
        left_hess_val_vect[c_id] += hess_vect[col_id];
      }
      else {
        right_grad_val_vect[c_id] += grad_vect[col_id];
        right_hess_val_vect[c_id] += hess_vect[col_id];
      }
    }
  }

  // Step 2: Push grad and hess to PS
  std::vector<float> push_val_vect;
  push_val_vect.insert(push_val_vect.end(), left_grad_val_vect.begin(), left_grad_val_vect.end());
  push_val_vect.insert(push_val_vect.end(), left_hess_val_vect.begin(), left_hess_val_vect.end());
  push_val_vect.insert(push_val_vect.end(), right_grad_val_vect.begin(), right_grad_val_vect.end());
  push_val_vect.insert(push_val_vect.end(), right_hess_val_vect.begin(), right_hess_val_vect.end());

  std::vector<Key> push_key_vect(push_val_vect.size());
  std::iota(push_key_vect.begin(), push_key_vect.end(), ps_key_ptr);
  ps_key_ptr += push_key_vect.size();

  //LOG(INFO) << "push_key_vect.size() = " << push_key_vect.size();
  //LOG(INFO) << "push_val_vect.size() = " << push_val_vect.size();

  _push_val_vect = push_val_vect;
  return push_key_vect;
}

std::map<std::string, float> GBDTTree::find_best_split(std::vector<float> grad_hess_vect, float complexity_of_leaf) {

  // Step 1: Decode vect from PS
  std::vector<float> left_grad_val_vect(grad_hess_vect.begin(), grad_hess_vect.begin() + (grad_hess_vect.size() / 4));
  std::vector<float> left_hess_val_vect(grad_hess_vect.begin() + (grad_hess_vect.size() / 4), grad_hess_vect.begin() + (grad_hess_vect.size() / 2));
  std::vector<float> right_grad_val_vect(grad_hess_vect.begin() + (grad_hess_vect.size() / 2), grad_hess_vect.begin() + (grad_hess_vect.size() * 3 / 4));
  std::vector<float> right_hess_val_vect(grad_hess_vect.begin() + (grad_hess_vect.size() * 3 / 4), grad_hess_vect.begin() + grad_hess_vect.size());

  // Step 2: Find best split
  int best_candidate_id = -1;
  float best_split_gain = -std::numeric_limits<float>::infinity();
  int candidate_num = grad_hess_vect.size() / 4;
  for (int c_id = 0; c_id < candidate_num; c_id++) {
    float left_grad_sum = left_grad_val_vect[c_id];
    float left_hess_sum = left_hess_val_vect[c_id];
    float right_grad_sum = right_grad_val_vect[c_id];
    float right_hess_sum = right_hess_val_vect[c_id];
    float grad_sum = left_grad_sum + right_grad_sum;
    float hess_sum = left_hess_sum + right_hess_sum;

    float left_result = ((left_grad_sum * left_grad_sum) / (left_hess_sum + complexity_of_leaf));
    float right_result = ((right_grad_sum * right_grad_sum) / (right_hess_sum + complexity_of_leaf));
    float orig_result = ((grad_sum * grad_sum) / (hess_sum + complexity_of_leaf));
    
    float split_gain = left_result + right_result - orig_result;
    if (split_gain > best_split_gain) {
      best_split_gain = split_gain;
      best_candidate_id = c_id;
      }
  }
  std::map<std::string, float> res;
  res["best_candidate_id"] = (float) best_candidate_id;
  res["best_split_gain"] = best_split_gain;

  return res;
}

bool GBDTTree::check_to_stop() {
  // 1. Check depth
  if (this->depth >= this->params["max_depth"]) {
    return true;
  }

  // 2. Check min gain
  

  return false;
}

void GBDTTree::set_depth(int depth) {
  this->depth = depth;
}

float GBDTTree::predict(std::vector<float> vect) {
  if (this->is_leaf) {
    return this->predict_val;
  }
  else {
    if (vect[this->feat_id] < this->split_val) {
      return this->left_child->predict(vect);
    }
    else {
      return this->right_child->predict(vect);
    }
  }
}

}
