package com.ciwei.canal.controller.handler;

import com.ciwei.canal.model.response.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletResponse;

/**
 * @author fuhang
 * @description: 全局异常处理
 * @date 2020/5/26 11:26
 */
@ControllerAdvice
public class ControllerHandler {
    private static final Logger logger = LoggerFactory.getLogger(ControllerHandler.class);

    @ExceptionHandler
    @ResponseBody
    public Object exceptionHandler(Exception e, HttpServletResponse response) {
        logger.error("unknown_error", e);
        return new Response<>(2, e.getMessage(), null).toString();
    }
}
