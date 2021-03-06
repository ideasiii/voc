package com.voc.api.model;

public class ArticleModel {
	private boolean success;
	private Article result;

	public boolean isSuccess() {
		return success;
	}
	public void setSuccess(boolean success) {
		this.success = success;
	}
	public Article getResult() {
		return result;
	}
	public void setResult(Article result) {
		this.result = result;
	}

	// ------------------------------------------------------

	public class Article {
		private String post_id;
		private String url;
		private String title;
		private String content;
		private String author;
		private String date; // yyyy-MM-dd HH:mm:ss (EX: 2018-06-10 19:49:47)
		private String channel;
		private Integer comment_count;
		private Integer like_count;
		private Integer share_count;
		private Integer view_count;
		private Integer reputation;
		private Double sentiment_score;
		private Integer sentiment;

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
		public String getContent() {
			return content;
		}
		public void setContent(String content) {
			this.content = content;
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
		public Integer getComment_count() {
			return comment_count;
		}
		public void setComment_count(Integer comment_count) {
			this.comment_count = comment_count;
		}
		public Integer getLike_count() {
			return like_count;
		}
		public void setLike_count(Integer like_count) {
			this.like_count = like_count;
		}
		public Integer getShare_count() {
			return share_count;
		}
		public void setShare_count(Integer share_count) {
			this.share_count = share_count;
		}
		public Integer getView_count() {
			return view_count;
		}
		public void setView_count(Integer view_count) {
			this.view_count = view_count;
		}
		public Integer getReputation() {
			return reputation;
		}
		public void setReputation(Integer reputation) {
			this.reputation = reputation;
		}
		public Double getSentiment_score() {
			return sentiment_score;
		}
		public void setSentiment_score(Double sentiment_score) {
			this.sentiment_score = sentiment_score;
		}
		public Integer getSentiment() {
			return sentiment;
		}
		public void setSentiment(Integer sentiment) {
			this.sentiment = sentiment;
		}
	}
}
