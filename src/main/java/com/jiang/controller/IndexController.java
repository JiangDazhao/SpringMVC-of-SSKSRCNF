package com.jiang.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class IndexController {
    //main page
    @RequestMapping("/Index/index")
    public String show(){
        return "index";
    }

    //upload page
    @RequestMapping("/Index/upload")
    public String Upload(){
        return  "upload";
    }

    //classify page
    @RequestMapping("/Index/classify")
    public String Classify(){
        return "classify";
    }
}
