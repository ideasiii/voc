package com.voc.api.model;

import java.util.ArrayList;
import java.util.List;

import com.voc.api.model.ArticleModel.Article;

public class ArticleListModel {
	private boolean success;
	private int total;
	private int page_num; 
	private int page_size;
	private List<Article> result = new ArrayList<>();

	public boolean isSuccess() {
		return success;
	}
	public void setSuccess(boolean success) {
		this.success = success;
	}
	public int getTotal() {
		return total;
	}
	public void setTotal(int total) {
		this.total = total;
	}
	public int getPage_num() {
		return page_num;
	}
	public void setPage_num(int page_num) {
		this.page_num = page_num;
	}
	public int getPage_size() {
		return page_size;
	}
	public void setPage_size(int page_size) {
		this.page_size = page_size;
	}
	public List<Article> getResult() {
		return result;
	}
	public void setResult(List<Article> result) {
		this.result = result;
	}
}
