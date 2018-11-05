package com.voc.servlet;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@WebServlet(name = "StartupServlet", urlPatterns = {"/StartupServlet"}, loadOnStartup = 1)
public class StartupServlet extends HttpServlet {
	private static final long serialVersionUID = -307475523890285345L;
	private static final Logger LOGGER = LoggerFactory.getLogger(StartupServlet.class);

	public void init() {
		LOGGER.info(" ************** VOC Start... ************** ");
	}

}
