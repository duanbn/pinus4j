$(document).ready(function(){

	$('.nav_item').click(function() {
		if($(this).hasClass('arrow_current')){
			$(this).toggleClass('arrow_current').next('ul').slideToggle(300);
		}
		else{
			$('.nav_item').removeClass('arrow_current').next('ul').slideUp(300);
			$(this).toggleClass('arrow_current').next('ul').slideToggle(300);
		}
	});	

});







//$(document).ready(function(){
//
//	$('.ul-nav > li > a').click(function() {  
//		var showUL = $(this).siblings('ul');
//		if (showUL.is(":hidden")) {
// 			showUL.slideToggle(300).slideUp("slow");
//			$(this).removeClass('open');
//			$(this).addClass('close');
//			return false;
//		}  
//		else {  
//			showUL.hide(); 
//			$(this).removeClass('close');
//			$(this).addClass('open');
//			return false;       
//		}  
//	});  
//
//});

	
