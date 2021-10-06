package com.jiang.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.HashMap;
import java.util.Map;

import com.jiang.service.ClassifyOnSpark;

@Controller
@ResponseBody
public class ClassifyController {

    @RequestMapping("/Classify/classify")
    public Map<String,Object> classify(){
        double OA;
        Map<String,Object> map= new HashMap<String,Object>();
        ClassifyOnSpark classifyclass= new ClassifyOnSpark();
        classifyclass.process();
        OA=classifyclass.getResult();
        map.put("OA",OA);
        return map;
    }
}
