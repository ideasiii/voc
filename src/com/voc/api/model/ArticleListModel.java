package com.voc.api.model;

import java.util.ArrayList;
import java.util.List;

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

	//------------------------------------------------------
	
	public class Article {
		private String post_id;
		private String url;
		private String title;
		private String author;
		private String date; // yyyy-MM-dd HH:mm:ss (EX: 2018-06-10 19:49:47)
		private String channel;
		private int comment_count;
		
		public String getPost_id() {
			return post_id;
		}
		public void setPost_id(String post_id) {
			this.post_id = post_id;
		}
		public String getUrl() {
			return url;
		}
		public void setUrl(String url) {
			this.url = url;
		}
		public String getTitle() {
			return title;
		}
		public void setTitle(String title) {
			this.title = title;
		}
		public String getAuthor() {
			return author;
		}
		public void setAuthor(String author) {
			this.author = author;
		}
		public String getDate() {
			return date;
		}
		public void setDate(String date) {
			this.date = date;
		}
		public String getChannel() {
			return channel;
		}
		public void setChannel(String channel) {
			this.channel = channel;
		}
		public int getComment_count() {
			return comment_count;
		}
		public void setComment_count(int comment_count) {
			this.comment_count = comment_count;
		}
	}
}
