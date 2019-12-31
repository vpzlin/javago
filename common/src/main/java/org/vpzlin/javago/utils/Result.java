package org.vpzlin.javago.utils;

public class Result {
    private boolean isSuccess;
    private Object data;
    private String message;

    public boolean isSuccess() {
        return isSuccess;
    }

    public Object getData() {
        return data;
    }

    public String getMessage() {
        return message;
    }

    public static Result getResult(boolean isSuccess, Object data, String message){
        Result result = new Result();
        result.isSuccess = isSuccess;
        result.data = data;
        result.message = message;

        return result;
    }
}