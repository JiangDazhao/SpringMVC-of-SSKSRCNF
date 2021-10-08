<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<!DOCTYPE html>
<html lang="en">
<head>
    <title>高光谱分类并行系统</title>

    <meta charset="UTF-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1.0"/>

    <link href="${pageContext.request.contextPath}/lib/bootstrap/bootstrap-3.3.7.css" rel="stylesheet">
    <link rel="stylesheet" href="${pageContext.request.contextPath}/lib/bootstrap/bootstrap-responsive.min.css"/>
    <link rel="stylesheet" href="${pageContext.request.contextPath}/lib/matrix/matrix-media.css"/>
    <link rel="stylesheet" href="${pageContext.request.contextPath}/lib/matrix/matrix-style.css"/>
    <link rel="stylesheet" href="${pageContext.request.contextPath}/lib/font-awesome/font-awesome.css"/>

    <!-- 实现左边动画 -->
    <script type="text/javascript" src="${pageContext.request.contextPath}/lib/jquery/jquery.min.js"></script>

    <script src="${pageContext.request.contextPath}/lib/matrix/matrix.js"></script>

    <script type="text/javascript" src="${pageContext.request.contextPath}/lib/bootstrap/bootstrap3.3.7.js"></script>
</head>
<body>

<!--顶部-->
<div id="user-nav" class="navbar-inverse" style="">
    <ul class="nav" style="height:35px;margin-top: -5px">
    </ul>
</div>

<!--左侧导航栏-->
<div id="sidebar" style="margin-top: 30px">
    <div id="headers">
        <div style="float:left;height:50px;background-color:#323232;margin-top: 230px">
            <div style="float:left;background-color:rgb(39,169,227);height:50px;width:220px;">
                <a href="${pageContext.request.contextPath}/Index/index">
                    <p style="font-size:30px;text-align:center;color: black">回到首页</p>
                </a>
            </div>
        </div>

    </div>
    <ul>
        <li class="submenu">
            <a href="#">
                <i class="icon icon-group"></i>
                <span>高光谱图像入库</span>
            </a>
            <ul>
                <li><a href="${pageContext.request.contextPath}/Index/upload">输入数据</a></li>
            </ul>
        </li>
        <li class="submenu">
            <a href="#">
                <i class="icon icon-signal"></i>
                <span>高光谱图像并行分类</span>
            </a>
            <ul>
                <li><a href="${pageContext.request.contextPath}/Index/classify">并行分类</a></li>
            </ul>
        </li>
    </ul>
</div>


<%--中心分类--%>
<div id="content" style="margin-right: 100px;margin-top: 40px;">
    <button class="btn btn-success btn-default" onclick="classify()">执行</button>
    <div>
        <ul>分类正确率：</ul>
        <ul id="result"></ul>
    </div>
</div>


<script>
    function classify() {
        $.post(
        {
            url:"${pageContext.request.contextPath}/Classify/classify",
            success:function (data) {
                console.log(data['OA']);
            }
        }
        )
    }



</script>


</body>
</html>
