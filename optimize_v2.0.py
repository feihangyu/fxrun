from urllib import request

import pymysql
import cv2
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import datetime
import time

from max_min_filter import max_min_filter

plt.rcParams["font.sans-serif"] = ["SimHei"]
plt.rcParams["axes.unicode_minus"] = False


# 识别图像二维码
# 可以识别二维码获取shelf_id 与 爬虫的shelf_id 比较,判断是否一致,来识别店主是否乱上传图片.
def detect_qrcode(img):
    qrDecoder = cv2.QRCodeDetector()
    data, bbox, rectifiedImage = qrDecoder.detectAndDecode(img)
    return 1 if len(data) > 0 else 0


# 通过HSV识别货架橙色区域
def detect_shelf(img):
    lower_orange = np.array([11, 43, 46])
    upper_orange = np.array([25, 255, 255])
    hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)
    mask = cv2.inRange(hsv, lower_orange, upper_orange)
    # cv2.imshow('mask', mask)
    # 膨胀处理
    element2 = cv2.getStructuringElement(cv2.MORPH_RECT, (6, 3))
    # 膨胀一次，让轮廓突出
    dilation = cv2.dilate(mask, element2, iterations=1)
    cv2.imshow('hsv_mask', dilation)
    return dilation


# 找出货架轮廓
def findShelfRegion(binary_img, img):
    region = []
    box_dict = {}
    box_y_dict = {}
    max_ratio = 0
    max_box = []
    contours = recognizeContour(binary_img)
    # 筛选货架横条(面积>3000,宽高比>1.3)
    for i in range(len(contours)):
        cnt = contours[i]
        # 计算该轮廓的面积
        area = cv2.contourArea(cnt)

        # 面积小的都筛选掉
        if (area > 3000):
            # box是四个点的坐标
            rect = cv2.minAreaRect(cnt)  # 生成最小的外接矩形
            box = cv2.boxPoints(rect)
            box = np.int0(box)

            # 计算box的高和宽
            height = abs(box[0][1] - box[2][1])
            width = abs(box[0][0] - box[2][0])
            # 宽高比
            ratio = float(width) / float(height)
            # 筛选货架横条,即扁的矩形(宽高比>1.3)
            if (ratio > 1.3):  # 7
                region.append(box)
                # 保存宽高比最高的box,作为轮廓的宽度
                if ratio > max_ratio:
                    max_ratio = ratio
                    max_box = box

    # 当货架识别不到轮廓或二维码图片,提前结束
    if detect_qrcode(img) == 1:
        response_data = {'flag': 2}
        return response_data
    if len(max_box) == 0:
        response_data = {'flag': 3}
        return response_data
    # 画出这些找到的货架横条轮廓(红色)
    img_org1 = img.copy()
    for box in region:
        shelf_image = cv2.drawContours(img_org1, [box], 0, (0, 0, 255), 2)
        cv2.imshow('shelf_image', shelf_image)

    # 货架的x轴坐标定位(轮廓宽度)
    xs = [max_box[0, 0], max_box[1, 0], max_box[2, 0], max_box[3, 0]]
    xs_sorted_index = np.argsort(xs)
    x1 = (max_box[xs_sorted_index[0]][0] + max_box[xs_sorted_index[1]][0]) // 2
    x2 = (max_box[xs_sorted_index[2]][0] + max_box[xs_sorted_index[3]][0]) // 2

    # 货架的x轴坐标定位(轮廓高度)
    # 先获取最上面的box和最下面的box
    for i in range(len(region)):
        box = region[i]
        ys = [box[0, 1], box[1, 1], box[2, 1], box[3, 1]]
        ys_sorted_index = np.argsort(ys)
        box_dict[i] = box
        box_y_dict[i] = box[ys_sorted_index[0], 1]
    max_index = sorted(box_y_dict.items(), key=lambda x: x[1], reverse=True)[0][0]
    min_index = sorted(box_y_dict.items(), key=lambda x: x[1])[0][0]
    upper_box = box_dict[min_index]
    lower_box = box_dict[max_index]

    # y1 = 上面的box中间高度， y2 = 最下面的box下面高度
    # (优化) 如果len(box) >= 5, y1 = 上面的box下面高度
    # 求y1
    up_ys = [upper_box[0, 1], upper_box[1, 1], upper_box[2, 1], upper_box[3, 1]]
    up_ys_ind = np.argsort(up_ys)
    if len(region) >= 5:
        y1 = (upper_box[up_ys_ind[2]][1] + upper_box[up_ys_ind[3]][1]) // 2
    else:
        y1 = (upper_box[up_ys_ind[0]][1] + upper_box[up_ys_ind[2]][1]) // 2

    # 求y2
    low_ys = [lower_box[0, 1], lower_box[1, 1], lower_box[2, 1], lower_box[3, 1]]
    low_ys_ind = np.argsort(low_ys)
    y2 = (lower_box[low_ys_ind[2]][1] + lower_box[low_ys_ind[3]][1]) // 2

    img_org2 = img.copy()
    shelf_region = np.array([[x1, y1], [x1, y2], [x2, y2], [x2, y1]])

    # 绘制图片货架轮廓
    img_shelf_region = cv2.drawContours(img_org2, [shelf_region], 0, (0, 0, 255), 2)
    cv2.imshow('img_shelf_region', img_shelf_region)

    # 返回数据,判断图片是否货架图片\二维码\异常图片
    if len(region) >= 4:
        flag = 1    # 图片正常
    else:
        flag = 3    # 图片异常（满足以下条件之一：1、货架图片不全；2、货架玻璃反光严重（曝光）；3、货架光线不足（阴暗）；4、图片像素过低；5、非货架图片；6、货架前有遮挡物）
    response_data = {
        'flag': flag,
        'coordinate': (x1, x2, y1, y2)
    }
    return response_data

# 抠图
def cutOut(binary_img, img):
    # 设置掩模，将图像处理聚焦到货架
    mask = np.zeros(img.shape[:2], np.uint8)
    x1, x2, y1, y2 = findShelfRegion(binary_img, img)['coordinate']
    # 掩模尺寸(两张图片按位与相加)
    mask[y1:y2, x1:x2] = 255
    masked_img = cv2.bitwise_and(img, img, mask=mask)
    cv2.imshow('masked_img',masked_img)
    return mask, masked_img


# 图像处理
def imgProcess(masked_img):
    # 将图片转化成灰度图
    gray = cv2.cvtColor(masked_img, cv2.COLOR_BGR2GRAY)
    # 中值滤波
    median = cv2.medianBlur(gray, 3)
    # 最大最小值滤波器
    max_min = max_min_filter(median)
    # canny边缘检测
    canny = cv2.Canny(max_min,50,255,apertureSize=3)
    # 膨胀和腐蚀操作的核函数
    element1 = cv2.getStructuringElement(cv2.MORPH_RECT, (6, 1))
    element2 = cv2.getStructuringElement(cv2.MORPH_RECT, (6, 3))
    # 膨胀一次，让轮廓突出
    dilation = cv2.dilate(canny, element2, iterations=1)
    # 腐蚀一次，去掉细节
    erosion = cv2.erode(dilation, element1, iterations=1)
    # 再次膨胀，让轮廓明显一些
    dilation2 = cv2.dilate(erosion, element2, iterations=3)
    return dilation2


# 识别货架商品轮廓
def recognizeContour(binary_img):
    contours, hierarchv = cv2.findContours(binary_img, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
    return contours


# 绘制货架商品轮廓
def imgContour(binary_img, img):
    contours = recognizeContour(binary_img)
    Image = cv2.drawContours(img, contours, -1, (0, 255, 0), 1)
    cv2.imshow('contours', Image)


# 计算轮廓总面积
def areaCal(contour):
    area = 0
    for i in range(len(contour)):
        area += cv2.contourArea(contour[i])
    return area


# 计算货架空置率预测结果
def shelfVacancyRate(binary_img,response_data):
    x1, x2, y1, y2 = response_data['coordinate']
    total_area = (y2 - y1) * (x2 - x1)
    contours = recognizeContour(binary_img)
    area = areaCal(contours)
    print(f'货架总面积：{total_area}，有库存面积：{0.75 * area}')
    # 0.08 * x *(w_end - w_start) 为每层识别的横条面积,需扣除
    print('货架空置率：%.2f' % (1 - ((0.75 * area)  / total_area)))


if __name__ == '__main__':
    start_time = time.time()
    # img = cv2.imread(r'image\26.jpg') # 二维码

    img = cv2.imread(r'image\11.jpg')
    tmp = img.copy()
    # hsv二值化图片
    hsv_mask = detect_shelf(img)
    response_data = findShelfRegion(hsv_mask, img)

    # 判读图片是否异常
    if response_data['flag'] == 1:
        # 抠图
        mask, masked_img = cutOut(hsv_mask, img)
        # 图片处理
        dilation2 = imgProcess(masked_img)
        # 绘制轮廓
        imgContour(dilation2,img)
        shelfVacancyRate(dilation2,response_data)

    elif response_data['flag'] == 2:
        print('图像识别结果：货架二维码')
    else:
        print('图片异常（满足以下条件之一：1、货架图片不全；2、货架玻璃反光严重（曝光）；'
              '3、货架光线不足（阴暗）；4、图片像素过低；5、非货架图片；6、货架前有遮挡物；7、二维码图片无法识别）')

    end_time = time.time()
    print(f'程序运行时间:{round(end_time - start_time, 2)} s')
    # 关闭窗口
    cv2.waitKey(0)
    cv2.destroyAllWindows()
