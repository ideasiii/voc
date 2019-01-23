package com.voc.servlet;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.voc.tool.MyJiebaSegmenter;

@WebServlet(name = "StartupServlet", urlPatterns = {"/StartupServlet"}, loadOnStartup = 1)
public class StartupServlet extends HttpServlet {
	private static final long serialVersionUID = -307475523890285345L;
	private static final Logger LOGGER = LoggerFactory.getLogger(StartupServlet.class);

	@Override
    public void init(ServletConfig config) throws ServletException {
		LOGGER.info(" ************** VOC Start... ************** ");
		MyJiebaSegmenter.getInstance(); // Initialize JiebaSegmenter Object (Singleton).
	}

}
