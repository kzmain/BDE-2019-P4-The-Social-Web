// --------System Level Config --------
let true_div_id   = "container";
let true_bg_color = "rgb(46,47,51)";
let app = {};
let true_dom   = document.getElementById(true_div_id);
let true_chart = echarts.init(true_dom);
let true_emphasis_style = {
    itemStyle: {
        barBorderWidth: 1,
        shadowBlur: 10,
        shadowOffsetX: 0,
        shadowOffsetY: 0,
        shadowColor: 'rgba(0,0,0,0.5)'
    }
};
// ----------------Data----------------
// let true_x_axis_data = true_x_axis_data;
// let true_series_sentiment_analysis_hillary_positive
// let true_data_positive_hillary = [];
// let true_data_neutral_hillary = [];
// let true_data_negative_hillary = [];
// let true_data_positive_trump = [];
// let true_data_neutral_trump = [];
// let true_data_negative_trump = [];
// let true_data_heat_hillary = [];
// let true_data_heat_trump = [];
// let true_data_estimation_hillary = [];
// let true_data_estimation_trump = [];

// --------------Position--------------
let left_center = 'center';
let true_title_overall_top = 20;
let true_title_overall_lef = left_center;

let true_title_estimation_top = 90;
let true_title_estimation_lef = left_center;

let true_grid_estimation_top = 130;
let true_grid_estimation_hei = 100;
let true_grid_estimation_wid = "84%";
let true_grid_estimation_lef = left_center;

let true_legend_estimation_top = 260;
let true_legend_estimation_lef = left_center;

let true_title_top_twitter_count_top = 310;
let true_title_top_twitter_count_lef = left_center;

let true_grid_top_twitter_count_top = 360;
let true_grid_top_twitter_count_hei = 100;
let true_grid_top_twitter_count_wid = "84%";
let true_grid_top_twitter_count_lef = left_center;

let true_legend_top_twitter_count_top = 490;
let true_legend_top_twitter_count_lef = left_center;

let true_title_sentiment_analysis_top = 540;
let true_title_sentiment_analysis_lef = left_center;

let true_grid_sentiment_analysis_hillary_top = 590;
let true_grid_sentiment_analysis_hillary_hei = 50;
let true_grid_sentiment_analysis_hillary_wid = "84%";
let true_grid_sentiment_analysis_hillary_lef = left_center;

let true_grid_sentiment_analysis_trump_top = 640;
let true_grid_sentiment_analysis_trump_hei = 50;
let true_grid_sentiment_analysis_trump_wid = "84%";
let true_grid_sentiment_analysis_trump_lef = left_center;

let true_visual_map_sentiment_analysis_top = 590;
let true_visual_map_sentiment_analysis_hei = "80%";
let true_visual_map_sentiment_analysis_wid = "10%";
let true_visual_map_sentiment_analysis_lef = "2%";

let true_legend_sentiment_analysis_top = 720;
let true_legend_sentiment_analysis_lef = left_center;

let true_data_zoom_all_x_axis_top = 750;
let true_data_zoom_all_x_axis_hei = 20;
let true_data_zoom_all_x_axis_wid = "84%";
let true_data_zoom_all_x_axis_lef = left_center;

let true_title_word_cloud_top = 800;
let true_title_word_cloud_lef = left_center;

let true_series_word_cloud_top = 850;
let true_series_word_cloud_hei = 200;
let true_series_word_cloud_wid = "84%";
let true_series_word_cloud_lef = left_center;
// ---------------Title ---------------
let title_1_size = 30;
let title_2_size=  20;
let true_title_overall            = {
    text: 'Social web influence on 2016 US president election',
    textStyle:{
        color: '#eee',
        fontSize: title_1_size
    },
    top:  true_title_overall_top,
    left: true_title_overall_lef,
};
let true_title_estimation         = {
    text: 'Election Estimation',
    textStyle:{
        color: '#eee',
        fontSize: title_2_size
    },
    top:  true_title_estimation_top,
    left: true_title_estimation_lef,
};
let true_title_top_twitter_count  = {
    text: 'Election Top Twitter Count',
    textStyle:{
        color: '#eee',
        fontSize: title_2_size
    },
    top:  true_title_top_twitter_count_top,
    left: true_title_top_twitter_count_lef,
};
let true_title_sentiment_analysis = {
    text: 'Election sentiment analysis',
    textStyle:{
        color: '#eee',
        fontSize: title_2_size
    },
    top:  true_title_sentiment_analysis_top,
    left: true_title_sentiment_analysis_lef,
};
let true_title_word_cloud         = {
    text: 'Candidates word cloud analysis',
    textStyle:{
        color: '#eee',
        fontSize: title_2_size
    },
    top:  true_title_word_cloud_top,
    left: true_title_word_cloud_lef,
};
let true_title_list = [
    true_title_overall,
    true_title_estimation,
    true_title_top_twitter_count,
    true_title_sentiment_analysis,
    true_title_word_cloud,
];
// ---------------Legend---------------
let true_legend_estimation          = {
    data: ['Hillary wins', 'Trump wins'],
    textStyle:{
        color: "#fff",
    },
    top: true_legend_estimation_top,
    left: true_legend_estimation_lef
};
let true_legend_top_twitter_count   = {
    data: ['Hillary', 'Trump'],
    textStyle:{
        color: "#fff",

    },
    top:  true_legend_top_twitter_count_top,
    left: true_legend_top_twitter_count_lef
};
let true_legend_sentiment_analysis  = {
    data: ['Positive', 'Neutral', 'Negative'],
    textStyle:{
        color: "#fff",

    },
    top:  true_legend_sentiment_analysis_top,
    left: true_legend_sentiment_analysis_lef
};
let true_legend_list = [
    true_legend_estimation,
    true_legend_top_twitter_count,
    true_legend_sentiment_analysis
];
// ----------------Grid----------------
let true_grid_estimation_index                 = 0;
let true_grid_top_twitter_count_index          = 1;
let true_grid_sentiment_analysis_hillary_index = 2;
let true_grid_sentiment_analysis_trump_index   = 3;
let true_grid_estimation                 = {
    left:   true_grid_estimation_lef,
    top:    true_grid_estimation_top,
    height: true_grid_estimation_hei,
    width:  true_grid_estimation_wid,
};
let true_grid_top_twitter_count          = {
    left:   true_grid_top_twitter_count_lef,
    top:    true_grid_top_twitter_count_top,
    height: true_grid_top_twitter_count_hei,
    width:  true_grid_top_twitter_count_wid,
};
let true_grid_sentiment_analysis_hillary = {
    left:   true_grid_sentiment_analysis_hillary_lef,
    top:    true_grid_sentiment_analysis_hillary_top,
    height: true_grid_sentiment_analysis_hillary_hei,
    width:  true_grid_sentiment_analysis_hillary_wid,
};
let true_grid_sentiment_analysis_trump   = {
    left:   true_grid_sentiment_analysis_trump_lef,
    top:    true_grid_sentiment_analysis_trump_top,
    height: true_grid_sentiment_analysis_trump_hei,
    width:  true_grid_sentiment_analysis_trump_wid,
};
let true_grid_list = [
    true_grid_estimation,
    true_grid_top_twitter_count,
    true_grid_sentiment_analysis_hillary,
    true_grid_sentiment_analysis_trump
];
// ---------------xAxis ---------------
let true_xAxis_estimation_index                 = 0;
let true_xAxis_top_twitter_count_index          = 1;
let true_xAxis_sentiment_analysis_hillary_index = 2;
let true_xAxis_sentiment_analysis_trump_index   = 3;
let true_xAxis_estimation                 = {
    data: true_x_axis_data,
    type: 'category',
    gridIndex: true_grid_estimation_index,
    axisLine: {
        onZero: true,
        lineStyle: {
                color: '#eee'
            }
    },
    splitLine: {show: false},
};
let true_xAxis_top_twitter_count          = {
    data: true_x_axis_data,
    type: 'category',
    gridIndex: true_grid_top_twitter_count_index,
    axisLine: {
        onZero: true,
        lineStyle: {
                color: '#eee'
            }
    },
    splitLine: {show: false},
};
let true_xAxis_sentiment_analysis_hillary = {
    data: true_x_axis_data,
    type: 'category',
    gridIndex: true_grid_sentiment_analysis_hillary_index,
    axisLine: {
        onZero: true,
        lineStyle: {
                color: '#eee'
            }
    },
    show: false,
    splitLine: {show: true},
};
let true_xAxis_sentiment_analysis_trump   = {
    data: true_x_axis_data,
    type: 'category',
    gridIndex: true_grid_sentiment_analysis_trump_index,
    axisLine: {
        onZero: true,
        lineStyle: {
                color: '#eee'
            }
    },
    splitLine: {show: false},
};
let true_xAxis_list = [
    true_xAxis_estimation,
    true_xAxis_top_twitter_count,
    true_xAxis_sentiment_analysis_hillary,
    true_xAxis_sentiment_analysis_trump,
];
// ---------------yAxis ---------------
let true_yAxis_estimation_index                 = 0;
let true_yAxis_top_twitter_count_index          = 1;
let true_yAxis_sentiment_analysis_hillary_index = 2;
let true_yAxis_sentiment_analysis_trump_index   = 3;
let true_yAxis_estimation                 = {
    min: 0,
    type: 'value',
    name: 'Electability (%)',
    nameLocation: 'center',
    nameTextStyle: {
        padding: [10, 10, 20, 10]
    },
    splitLine: {show: false},
    splitNumber: 4,
    gridIndex: true_grid_estimation_index,
    position: 'left',
    axisLine: {
        lineStyle: {
            color: '#eee'
        }
    },
};
let true_yAxis_top_twitter_count          = {
    min: 0,
    type: 'value',
    name: 'Count',
    nameLocation: 'center',
    nameTextStyle: {
        padding: [10, 10, 20, 10]
    },
    splitLine: {show: false},
    gridIndex: true_grid_top_twitter_count_index,
    position: 'left',
    axisLine: {
        lineStyle: {
            color: '#eee'
        }
    },
    // axisLabel: {
    //     formatter: '{value} %'
    // }
};
let true_yAxis_sentiment_analysis_hillary = {
    name: 'Hillary',
    nameLocation: 'top',
    nameTextStyle: {
        padding: [0, 0, 0, 70],
        color: "blue",
        fontSize: "20"
    },
    splitArea: {show: false},
    splitLine: {show: false},
    axisLine: {show: false},
    axisLabel: {show: false},
    show: true,
    gridIndex: true_grid_sentiment_analysis_hillary_index,
    position: 'right',
};
let true_yAxis_sentiment_analysis_trump   = {
    name: 'Trump',
    nameLocation: 'top',
    nameTextStyle: {
        padding: [0, 0, 0, 70],
        color: "red",
        fontSize: "20"
    },
    splitArea: {show: false},
    splitLine: {show: false},
    axisLine: {show: false},
    axisLabel: {show: false},
    show: true,
    gridIndex: true_grid_sentiment_analysis_trump_index,
    position: 'right',
};
let true_yAxis_list = [
    true_yAxis_estimation,
    true_yAxis_top_twitter_count,
    true_yAxis_sentiment_analysis_hillary,
    true_yAxis_sentiment_analysis_trump
];
// --------------Graphic --------------
// ---------------Series---------------
let true_series_sentiment_analysis_hillary_positive_index = 0;
let true_series_sentiment_analysis_hillary_neutral_index  = 1;
let true_series_sentiment_analysis_hillary_negative_index = 2;

let true_series_sentiment_analysis_trump_positive_index = 3;
let true_series_sentiment_analysis_trump_neutral_index  = 4;
let true_series_sentiment_analysis_trump_negative_index = 5;

let true_series_top_twitter_count_hillary_index = 6;
let true_series_top_twitter_count_trump_index   = 7;

let true_series_election_estimation_hillary_index = 8;
let true_series_election_estimation_trump_index   = 9;

let true_series_word_cloud_index = 10;
let true_series_markline_index = 11;

let true_series_sentiment_analysis_hillary_positive = {
    name: 'Positive',
    xAxisIndex: true_xAxis_sentiment_analysis_hillary_index,
    yAxisIndex: true_yAxis_sentiment_analysis_hillary_index,
    type: 'bar',
    stack: 'one',
    barCategoryGap: 0,
    barGap: 0,
    emphasis: true_emphasis_style,
    data: true_data_positive_hillary
};
let true_series_sentiment_analysis_hillary_neutral  = {
    name: 'Neutral',
    xAxisIndex: true_xAxis_sentiment_analysis_hillary_index,
    yAxisIndex: true_yAxis_sentiment_analysis_hillary_index,
    type: 'bar',
    stack: 'one',
    barCategoryGap: 0,
    barGap: 0,
    emphasis: true_emphasis_style,
    data: true_data_neutral_hillary
};
let true_series_sentiment_analysis_hillary_negative = {
    name: 'Negative',
    xAxisIndex: true_xAxis_sentiment_analysis_hillary_index,
    yAxisIndex: true_yAxis_sentiment_analysis_hillary_index,
    type: 'bar',
    stack: 'one',
    barCategoryGap: 0,
    barGap: 0,
    emphasis: true_emphasis_style,
    data: true_data_negative_hillary
};

let true_series_sentiment_analysis_trump_positive = {
    name: 'Positive',
    xAxisIndex: true_xAxis_sentiment_analysis_trump_index,
    yAxisIndex: true_yAxis_sentiment_analysis_trump_index,
    type: 'bar',
    stack: 'two',
    barCategoryGap: 0,
    barGap: 0,
    emphasis: true_emphasis_style,
    data: true_data_positive_trump
};
let true_series_sentiment_analysis_trump_neutral  = {
    name: 'Neutral',
    xAxisIndex: true_xAxis_sentiment_analysis_trump_index,
    yAxisIndex: true_yAxis_sentiment_analysis_trump_index,
    type: 'bar',
    stack: 'two',
    barCategoryGap: 0,
    barGap: 0,
    emphasis: true_emphasis_style,
    data: true_data_neutral_trump
};
let true_series_sentiment_analysis_trump_negative = {
    name: 'Negative',
    xAxisIndex: true_xAxis_sentiment_analysis_trump_index,
    yAxisIndex: true_yAxis_sentiment_analysis_trump_index,
    type: 'bar',
    stack: 'two',
    barCategoryGap: 0,
    barGap: 0,
    emphasis: true_emphasis_style,
    data: true_data_negative_trump
};

let true_series_top_twitter_count_hillary = {
    name: 'Hillary',
    xAxisIndex: true_xAxis_top_twitter_count_index,
    yAxisIndex: true_yAxis_top_twitter_count_index,
    type: 'line',
    // stack: 'two',
    emphasis: true_emphasis_style,
    data: true_data_heat_hillary
};
let true_series_top_twitter_count_trump   = {
    name: 'Trump',
    xAxisIndex: true_xAxis_top_twitter_count_index,
    yAxisIndex: true_yAxis_top_twitter_count_index,
    type: 'line',
    // stack: 'two',
    emphasis: true_emphasis_style,
    data: true_data_heat_trump
};

let true_series_election_estimation_hillary = {
    name: 'Hillary wins',
    xAxisIndex: true_xAxis_estimation_index,
    yAxisIndex: true_yAxis_estimation_index,
    type: 'bar',
    color: "blue",
    stack: 'three',
    emphasis: true_emphasis_style,
    data: true_data_estimation_hillary
};
let true_series_election_estimation_trump   = {
    name: 'Trump wins',
    xAxisIndex: true_xAxis_estimation_index,
    yAxisIndex: true_yAxis_estimation_index,
    type: 'bar',
    color: "red",
    stack: 'three',
    emphasis: true_emphasis_style,
    data: true_data_estimation_trump
};

let true_series_word_cloud = {
    type: 'wordCloud',
    gridSize: 10,
    sizeRange: [12, 30],
    rotationRange: [-3, 3],
    // shape: 'pentagon',
    // width: "100%",
    height: true_series_word_cloud_hei,
    width:  true_series_word_cloud_wid,
    left:   true_series_word_cloud_lef,
    top:    true_series_word_cloud_top,
    drawOutOfBound: true,
    textStyle: {
        normal: {
            color: function () {
                return 'rgb(' + [
                    Math.round(Math.random() * 200 + 50),
                    Math.round(Math.random() * 200 + 50),
                    Math.round(Math.random() * 200 + 50)
                ].join(',') + ')';
            }
        },
        emphasis: {
            shadowBlur: 3,
            shadowColor: '#333'
        }
    },
    data: true_data_word_cloud
};

let true_series_markline = {
    type: 'bar',
    name: 'Important dates',
    gridIndex:  true_grid_top_twitter_count_index,
    xAxisIndex: true_xAxis_top_twitter_count_index,
    yAxisIndex: true_yAxis_top_twitter_count_index,

    barCategoryGap: 0,
    barGap: 0,
    emphasis: [],
    data: [],
    markLine: {
        data: [
            {
                name: 'First presidential debate',
                xAxis: '2016-09-26'
            },
            {
                name: 'Russia Directed Hacks to Influence Elections',
                xAxis: '2016-10-07'
            },
            {
                name: 'Hillary Clinton case under review by FBI',
                xAxis: '2016-10-28'
            },
            {
                name: 'US Presidential Election 2016',
                xAxis: '2016-11-08'
            },
        ]
    }
};
let true_series_list = [
        true_series_sentiment_analysis_hillary_positive,
        true_series_sentiment_analysis_hillary_neutral,
        true_series_sentiment_analysis_hillary_negative,
        true_series_sentiment_analysis_trump_positive,
        true_series_sentiment_analysis_trump_neutral,
        true_series_sentiment_analysis_trump_negative,
        true_series_top_twitter_count_hillary,
        true_series_top_twitter_count_trump,
        true_series_election_estimation_hillary,
        true_series_election_estimation_trump,
        true_series_word_cloud,
    true_series_markline
    ];

// -------------Visual Map-------------
let true_visual_map_sentiment_analysis = {
    type: 'continuous',
    dimension: 1,
    textStyle:{
        color: "white"
    },
    calculable: true,
    min: 0,
    max: 100,
    top:        true_visual_map_sentiment_analysis_top,
    left:       true_visual_map_sentiment_analysis_lef,
    itemHeight: true_visual_map_sentiment_analysis_hei,
    itemWidth:      true_visual_map_sentiment_analysis_wid,
    color: "white",
    inRange: {
        colorLightness: [0.48, 0.8]
    },
    outOfRange: {
        color: '#bbb'
    },
    controller: {
        inRange: {
            color: 'grey'
        }
    },
    seriesIndex: [
        true_series_sentiment_analysis_hillary_positive_index,
        true_series_sentiment_analysis_hillary_neutral_index,
        true_series_sentiment_analysis_hillary_negative_index,
        true_series_sentiment_analysis_trump_positive_index,
        true_series_sentiment_analysis_trump_neutral_index,
        true_series_sentiment_analysis_trump_negative_index
    ]
};
let true_visual_map_list = [
    true_visual_map_sentiment_analysis,
    // true_visual_map_sentiment_analysis_trump
];
// -------------Data Zoom -------------
let true_data_zoom_start = 0;
let true_data_zoom_end = 100;
let true_data_zoom_all_x_axis = {
    show: true,
    start: true_data_zoom_start,
    end:   true_data_zoom_end,
    top:    true_data_zoom_all_x_axis_top,
    left:   true_data_zoom_all_x_axis_lef,
    width:  true_data_zoom_all_x_axis_wid,
    height: true_data_zoom_all_x_axis_hei,
    xAxisIndex: [
        true_xAxis_estimation_index,
        true_xAxis_top_twitter_count_index,
        true_xAxis_sentiment_analysis_hillary_index,
        true_xAxis_sentiment_analysis_trump_index
    ],
};
let true_data_zoom_all_inner_x_axis = {
    type: 'inside',
    start: true_data_zoom_start,
    end:   true_data_zoom_end,
    xAxisIndex: [
        true_xAxis_estimation_index,
        true_xAxis_top_twitter_count_index,
        true_xAxis_sentiment_analysis_hillary_index,
        true_xAxis_sentiment_analysis_trump_index
    ],
};

let true_data_zoom_list = [
    true_data_zoom_all_x_axis,
    true_data_zoom_all_inner_x_axis
];

true_option = {
    backgroundColor: true_bg_color,
    dataZoom:        true_data_zoom_list,
    title:           true_title_list,
    legend:          true_legend_list,
    tooltip: {},
    toolbox: {show: false},
    visualMap:       true_visual_map_list,
    xAxis:           true_xAxis_list,
    yAxis:           true_yAxis_list,
    grid:            true_grid_list,
    series:          true_series_list
};


true_chart.on("click", function(param){
    if(param.componentSubType === "wordCloud"){
        console.log(param.data);
        console.log(param.seriesName);
        console.log(param);
        let n_url = "https://en.wikipedia.org/wiki/" + param.data.name;
        n_url = n_url.replace(" ", "_");
        window.open(n_url);
    }
    if(param.componentType === "markLine" && param.seriesIndex === true_series_markline_index){
        let date_list = param.data.value.toString().split("-");
        let temp_start_date = new Date(2016, 7, 1,0,0,0,0);
        let mark_line_date = new Date(date_list[0], date_list[1] - 1, date_list[2], 0, 0, 0, 0);
        let days_difference = (mark_line_date - temp_start_date) / (1000 * 60 * 60 * 24);
        let temp_s_value_index = parseInt(days_difference -3);
        let temp_e_value_index = parseInt(days_difference +7);
        if(temp_e_value_index > 99){temp_e_value_index = 99;}
        console.log(true_x_axis_data[temp_s_value_index]);
        console.log(true_x_axis_data[temp_e_value_index]);
        true_chart.dispatchAction({
            type: 'dataZoom',
            startValue: true_x_axis_data[temp_s_value_index],
            endValue:   true_x_axis_data[temp_e_value_index]
        });
    }
});

true_chart.on("datazoom", function(){
    setTimeout(function(){
        let temp_option = true_chart.getOption();
        let temp_s = parseInt(temp_option["dataZoom"]["0"]["start"]);
        let temp_e = parseInt(temp_option["dataZoom"]["0"]["end"]);
        if(true_data_zoom_start === temp_s && true_data_zoom_end === temp_e){
            return 0;
        }
        true_data_zoom_start = temp_s;
        true_data_zoom_end   = temp_e;
        let temp_date = new Date(2016, 7, 1,0,0,0,0);
        let temp_s_date = temp_date.setDate(temp_date.getDate() + temp_s);
        let temp_e_date = temp_date.setDate(temp_date.getDate() + temp_e);

        let XHR = new XMLHttpRequest();
        var request_data = {"start_date": temp_s_date, "end_date": temp_e_date};

        XHR.open('POST', "/api/true");
        XHR.setRequestHeader('content-type', 'application/json');  //先open再设置请求头

        XHR.send(JSON.stringify(request_data));
        XHR.onreadystatechange = function(){
              if(XHR.readyState === 4 && XHR.status === 200){
                  let t_option = true_chart.getOption();
                  t_option.series[true_series_word_cloud_index].data = JSON.parse(XHR.responseText);
                  true_chart.setOption(t_option);
              }
        };
    }, 4000);
   });
function resize() {
    true_chart.resize();
}

if (true_option && typeof true_option === "object") {
    true_chart.setOption(true_option, true);
}

window.addEventListener("resize", myFunction);

function myFunction() {
  true_chart.resize();
}