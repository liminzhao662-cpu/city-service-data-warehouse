package com.portfolio.governance.api;

import com.portfolio.governance.service.ConflictException;
import com.portfolio.governance.service.ResourceNotFoundException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ApiExceptionHandler {
    @ExceptionHandler({IllegalArgumentException.class, MethodArgumentNotValidException.class})
    ProblemDetail badRequest(Exception exception) {
        ProblemDetail detail = ProblemDetail.forStatus(HttpStatus.BAD_REQUEST);
        detail.setTitle("请求参数错误");
        detail.setDetail(exception instanceof MethodArgumentNotValidException validation
                ? validation.getBindingResult().getAllErrors().get(0).getDefaultMessage()
                : exception.getMessage());
        return detail;
    }

    @ExceptionHandler(ResourceNotFoundException.class)
    ProblemDetail notFound(ResourceNotFoundException exception) {
        ProblemDetail detail = ProblemDetail.forStatus(HttpStatus.NOT_FOUND);
        detail.setTitle("资源不存在");
        detail.setDetail(exception.getMessage());
        return detail;
    }

    @ExceptionHandler(ConflictException.class)
    ProblemDetail conflict(ConflictException exception) {
        ProblemDetail detail = ProblemDetail.forStatus(HttpStatus.CONFLICT);
        detail.setTitle("状态冲突");
        detail.setDetail(exception.getMessage());
        return detail;
    }
}

